import type {
	HealthResponse,
	PostcodeAutocompleteResponse,
	PostcodeLookupResponse,
	AddressListResponse,
	AddressResponse
} from './types';

const BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

export class ApiError extends Error {
	status: number;
	detail: string;

	constructor(status: number, detail: string) {
		super(detail);
		this.status = status;
		this.detail = detail;
	}
}

async function get<T>(path: string, params?: Record<string, string | number>): Promise<T> {
	const url = new URL(path, BASE_URL);
	if (params) {
		for (const [key, value] of Object.entries(params)) {
			if (value !== undefined && value !== null && value !== '') {
				url.searchParams.set(key, String(value));
			}
		}
	}

	const response = await fetch(url.toString());

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

	autocomplete(q: string, limit: number = 8): Promise<PostcodeAutocompleteResponse> {
		return get<PostcodeAutocompleteResponse>('/postcodes/autocomplete', { q, limit });
	},

	lookupPostcode(postcode: string): Promise<PostcodeLookupResponse> {
		return get<PostcodeLookupResponse>(`/postcodes/${encodeURIComponent(postcode)}`);
	},

	searchAddresses(params: {
		q?: string;
		postcode?: string;
		street?: string;
		city?: string;
		page?: number;
		page_size?: number;
	}): Promise<AddressListResponse> {
		return get<AddressListResponse>('/addresses/search', params as Record<string, string | number>);
	},

	getAddress(id: number): Promise<AddressResponse> {
		return get<AddressResponse>(`/addresses/${id}`);
	}
};
