// TypeScript interfaces mirroring backend Pydantic schemas

export interface HealthResponse {
	status: string;
	database: string;
	postcode_count: number;
	address_count: number;
}

export interface PostcodeResponse {
	id: number;
	postcode: string;
	postcode_no_space: string;
	latitude: number | null;
	longitude: number | null;
	country_code: string | null;
	region_code: string | null;
	local_authority: string | null;
	is_terminated: boolean;
}

export interface PostcodeAutocompleteItem {
	postcode: string;
	postcode_no_space: string;
}

export interface PostcodeAutocompleteResponse {
	query: string;
	count: number;
	results: PostcodeAutocompleteItem[];
}

export interface AddressResponse {
	id: number;
	postcode_raw: string | null;
	house_number: string | null;
	house_name: string | null;
	flat: string | null;
	street: string | null;
	suburb: string | null;
	city: string | null;
	county: string | null;
	latitude: number | null;
	longitude: number | null;
	confidence: number | null;
	is_complete: boolean;
}

export interface PostcodeLookupResponse {
	postcode: PostcodeResponse;
	address_count: number;
	addresses: AddressResponse[];
}

export interface AddressListResponse {
	count: number;
	total: number;
	page: number;
	page_size: number;
	results: AddressResponse[];
}

export interface ErrorResponse {
	detail: string;
}
