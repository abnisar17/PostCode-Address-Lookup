import type {
	HealthResponse,
	PostcodeAutocompleteResponse,
	PostcodeLookupResponse,
	AddressListResponse,
	AddressResponse
} from './types';

const BASE_URL = (import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000') + '/api';
// Optional API key, baked in at build time. Sent as X-API-Key so the public
// site keeps working when the backend has REQUIRE_API_KEY enabled.
const API_KEY: string | undefined = import.meta.env.VITE_API_KEY;

export class ApiError extends Error {
	status: number;
	detail: string;

	constructor(status: number, detail: string) {
		super(detail);
		this.status = status;
		this.detail = detail;
	}
}

async function get<T>(
	path: string,
	params?: Record<string, string | number>,
	signal?: AbortSignal
): Promise<T> {
	const url = new URL(BASE_URL + path);
	if (params) {
		for (const [key, value] of Object.entries(params)) {
			if (value !== undefined && value !== null && value !== '') {
				url.searchParams.set(key, String(value));
			}
		}
	}

	const headers: Record<string, string> = {};
	if (API_KEY) {
		headers['X-API-Key'] = API_KEY;
	}

	const response = await fetch(url.toString(), { signal, headers });

	if (!response.ok) {
		const body = await response.json().catch(() => ({ detail: response.statusText }));
		throw new ApiError(response.status, body.detail || response.statusText);
	}

	return response.json() as Promise<T>;
}

export const api = {
	health(): Promise<HealthResponse> {
		return get<HealthResponse>('/health');
	},

	autocomplete(
		q: string,
		limit: number = 8,
		signal?: AbortSignal
	): Promise<PostcodeAutocompleteResponse> {
		return get<PostcodeAutocompleteResponse>('/postcodes/autocomplete', { q, limit }, signal);
	},

	lookupPostcode(
		postcode: string,
		page: number = 1,
		page_size: number = 20,
		signal?: AbortSignal
	): Promise<PostcodeLookupResponse> {
		return get<PostcodeLookupResponse>(
			`/postcodes/${encodeURIComponent(postcode)}`,
			{ page, page_size },
			signal
		);
	},

	searchAddresses(
		params: {
			q?: string;
			postcode?: string;
			street?: string;
			city?: string;
			page?: number;
			page_size?: number;
		},
		signal?: AbortSignal
	): Promise<AddressListResponse> {
		return get<AddressListResponse>(
			'/addresses/search',
			params as Record<string, string | number>,
			signal
		);
	},

	getAddress(id: number): Promise<AddressResponse> {
		return get<AddressResponse>(`/addresses/${id}`);
	},

	submitAddress(payload: {
		postcode: string;
		house_number?: string;
		house_name?: string;
		flat?: string;
		street?: string;
		city?: string;
		county?: string;
	}): Promise<{ detail: string; id: number }> {
		const headers: Record<string, string> = { 'Content-Type': 'application/json' };
		if (API_KEY) {
			headers['X-API-Key'] = API_KEY;
		}
		return fetch(BASE_URL + '/addresses/submit', {
			method: 'POST',
			headers,
			body: JSON.stringify(payload)
		}).then(async (res) => {
			if (!res.ok) {
				const body = await res.json().catch(() => ({ detail: res.statusText }));
				throw new ApiError(res.status, body.detail || res.statusText);
			}
			return res.json();
		});
	}
};
