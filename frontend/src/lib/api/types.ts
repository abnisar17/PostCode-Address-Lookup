// TypeScript interfaces mirroring backend Pydantic schemas

export interface HealthResponse {
	status: string;
	database: string;
	postcode_count: number;
	address_count: number;
	price_paid_count: number;
	company_count: number;
	food_rating_count: number;
	voa_rating_count: number;
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

// ── Enrichment types ────────────────────────────────────────────

export interface PricePaidResponse {
	id: number;
	transaction_id: string;
	price: number;
	date_of_transfer: string | null;
	property_type: string | null;
	old_new: string | null;
	duration: string | null;
}

export interface CompanyResponse {
	id: number;
	company_number: string;
	company_name: string | null;
	company_status: string | null;
	company_type: string | null;
	sic_code_1: string | null;
	incorporation_date: string | null;
}

export interface FoodRatingResponse {
	id: number;
	fhrs_id: number;
	business_name: string | null;
	business_type: string | null;
	rating_value: string | null;
	rating_date: string | null;
	scores_hygiene: number | null;
	scores_structural: number | null;
	scores_management: number | null;
}

export interface VOARatingResponse {
	id: number;
	uarn: number;
	description_text: string | null;
	firm_name: string | null;
	rateable_value: number | null;
	effective_date: string | null;
}

// ── Address types ───────────────────────────────────────────────

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
	source: string | null;
	uprn: number | null;
	price_paid?: PricePaidResponse[];
	companies?: CompanyResponse[];
	food_ratings?: FoodRatingResponse[];
	voa_ratings?: VOARatingResponse[];
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
