<script lang="ts">
	import type { AddressResponse } from '$lib/api/types';
	import { api } from '$lib/api/client';

	let { address }: { address: AddressResponse } = $props();

	// For search results: lazy-load enrichment when user clicks "View details"
	let enrichedAddress: AddressResponse = $state(address);
	let loadingDetail = $state(false);
	let detailLoaded = $state(false);
	let detailError = $state<string | null>(null);
	let prevAddressId = $state(address.id);

	// Only reset when the actual address changes (different ID), not on every re-render
	$effect(() => {
		if (address.id !== prevAddressId) {
			prevAddressId = address.id;
			enrichedAddress = address;
			detailLoaded = false;
			detailError = null;
		}
	});

	async function loadDetail() {
		if (detailLoaded || loadingDetail) return;
		loadingDetail = true;
		detailError = null;
		try {
			const full = await api.getAddress(address.id);
			enrichedAddress = full;
			detailLoaded = true;
		} catch (e: any) {
			detailError = e.detail ?? 'Failed to load details';
		} finally {
			loadingDetail = false;
		}
	}

	let formatted = $derived(
		[
			address.flat,
			address.house_number,
			address.house_name,
			address.street,
			address.suburb,
			address.city,
			address.county,
			address.postcode_raw
		]
			.filter(Boolean)
			.join(', ')
	);

	let confidencePct = $derived(
		address.confidence != null ? Math.round(address.confidence * 100) : null
	);

	const sourceLabels: Record<string, string> = {
		osm: 'OSM',
		land_registry: 'Land Registry',
		epc: 'EPC',
		companies_house: 'Companies House',
		fsa: 'FSA',
		voa: 'VOA'
	};

	let sourceLabel = $derived(
		address.source ? (sourceLabels[address.source] ?? address.source) : null
	);

	const propertyTypeLabels: Record<string, string> = {
		D: 'Detached',
		S: 'Semi-detached',
		T: 'Terraced',
		F: 'Flat',
		O: 'Other'
	};

	const durationLabels: Record<string, string> = {
		F: 'Freehold',
		L: 'Leasehold'
	};

	let sortedPricePaid = $derived(
		[...(enrichedAddress.price_paid ?? [])].sort((a, b) => {
			if (!a.date_of_transfer) return 1;
			if (!b.date_of_transfer) return -1;
			return b.date_of_transfer.localeCompare(a.date_of_transfer);
		})
	);

	let hasPricePaid = $derived((enrichedAddress.price_paid?.length ?? 0) > 0);
	let hasCompanies = $derived((enrichedAddress.companies?.length ?? 0) > 0);
	let hasFoodRatings = $derived((enrichedAddress.food_ratings?.length ?? 0) > 0);
	let hasVoaRatings = $derived((enrichedAddress.voa_ratings?.length ?? 0) > 0);
	let hasEnrichment = $derived(hasPricePaid || hasCompanies || hasFoodRatings || hasVoaRatings);

	// True if enrichment data hasn't been loaded yet (search results without detail)
	let needsDetailLoad = $derived(
		!detailLoaded &&
			!address.price_paid &&
			!address.companies &&
			!address.food_ratings &&
			!address.voa_ratings
	);

	function formatRateableValue(value: number): string {
		return new Intl.NumberFormat('en-GB', {
			style: 'currency',
			currency: 'GBP',
			minimumFractionDigits: 0,
			maximumFractionDigits: 0
		}).format(value);
	}

	function formatPrice(price: number): string {
		return new Intl.NumberFormat('en-GB', {
			style: 'currency',
			currency: 'GBP',
			minimumFractionDigits: 0,
			maximumFractionDigits: 0
		}).format(price);
	}

	function formatDate(dateStr: string | null): string {
		if (!dateStr) return 'Unknown date';
		try {
			return new Date(dateStr).toLocaleDateString('en-GB', {
				day: 'numeric',
				month: 'short',
				year: 'numeric'
			});
		} catch {
			return dateStr;
		}
	}

	function ratingColor(rating: string | null): string {
		if (rating === null) return 'bg-gray-100 text-gray-700';
		const num = parseInt(rating, 10);
		if (isNaN(num)) return 'bg-gray-100 text-gray-700';
		if (num >= 4) return 'bg-green-100 text-green-800';
		if (num >= 2) return 'bg-yellow-100 text-yellow-800';
		return 'bg-red-100 text-red-800';
	}
</script>

<div class="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
	<p class="text-gray-900">{formatted || 'No address details'}</p>
	<div class="mt-2 flex flex-wrap items-center gap-3 text-xs text-gray-500">
		{#if sourceLabel}
			<span class="rounded-full bg-indigo-100 px-2 py-0.5 text-indigo-700">{sourceLabel}</span>
		{/if}
		{#if address.uprn != null}
			<span>UPRN: {address.uprn}</span>
		{/if}
		{#if confidencePct != null}
			<span>Confidence: {confidencePct}%</span>
		{/if}
{#if address.latitude != null && address.longitude != null}
			<span>{address.latitude.toFixed(4)}, {address.longitude.toFixed(4)}</span>
		{/if}
	</div>

	{#if needsDetailLoad}
		<div class="mt-3">
			<button
				class="flex items-center gap-1.5 rounded bg-indigo-50 px-3 py-1.5 text-xs font-medium text-indigo-700 hover:bg-indigo-100 disabled:opacity-50"
				onclick={loadDetail}
				disabled={loadingDetail}
			>
				{#if loadingDetail}
					<svg class="h-3 w-3 animate-spin" fill="none" viewBox="0 0 24 24">
						<circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
						<path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
					</svg>
					Loading...
				{:else}
					View details
				{/if}
			</button>
			{#if detailError}
				<p class="mt-1 text-xs text-red-500">{detailError}</p>
			{/if}
		</div>
	{/if}

	{#if detailLoaded && !hasEnrichment}
		<p class="mt-3 text-xs text-gray-400">No enrichment data linked to this address.</p>
	{/if}

	{#if hasEnrichment}
		<div class="mt-3 space-y-1">
			{#if hasPricePaid}
				<details class="group rounded border border-gray-100">
					<summary
						class="flex cursor-pointer items-center gap-2 px-3 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
					>
						<svg
							class="h-4 w-4 transition-transform group-open:rotate-90"
							fill="none"
							viewBox="0 0 24 24"
							stroke="currentColor"
							stroke-width="2"
						>
							<path stroke-linecap="round" stroke-linejoin="round" d="M9 5l7 7-7 7" />
						</svg>
						Price History
						<span class="ml-auto text-xs font-normal text-gray-400"
							>{sortedPricePaid.length}
							{sortedPricePaid.length === 1 ? 'transaction' : 'transactions'}</span
						>
					</summary>
					<div class="border-t border-gray-100 px-3 py-2">
						<ul class="space-y-1.5">
							{#each sortedPricePaid as txn (txn.id)}
								<li class="flex items-baseline justify-between text-sm">
									<div class="flex items-baseline gap-2">
										<span class="font-semibold text-gray-900"
											>{formatPrice(txn.price)}</span
										>
										<span class="text-xs text-gray-500"
											>{formatDate(txn.date_of_transfer)}</span
										>
									</div>
									<div class="flex gap-1.5 text-xs text-gray-400">
										{#if txn.property_type}
											<span
												>{propertyTypeLabels[txn.property_type] ??
													txn.property_type}</span
											>
										{/if}
										{#if txn.duration}
											<span
												>{durationLabels[txn.duration] ?? txn.duration}</span
											>
										{/if}
										{#if txn.old_new === 'Y'}
											<span class="text-blue-500">New build</span>
										{/if}
									</div>
								</li>
							{/each}
						</ul>
					</div>
				</details>
			{/if}

			{#if hasCompanies}
				<details class="group rounded border border-gray-100">
					<summary
						class="flex cursor-pointer items-center gap-2 px-3 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
					>
						<svg
							class="h-4 w-4 transition-transform group-open:rotate-90"
							fill="none"
							viewBox="0 0 24 24"
							stroke="currentColor"
							stroke-width="2"
						>
							<path stroke-linecap="round" stroke-linejoin="round" d="M9 5l7 7-7 7" />
						</svg>
						Companies
						<span class="ml-auto text-xs font-normal text-gray-400"
							>{enrichedAddress.companies?.length}
							{enrichedAddress.companies?.length === 1 ? 'company' : 'companies'}</span
						>
					</summary>
					<div class="border-t border-gray-100 px-3 py-2">
						<ul class="space-y-1.5">
							{#each enrichedAddress.companies ?? [] as company (company.id)}
								<li class="text-sm">
									<div class="flex items-baseline justify-between">
										<span class="font-medium text-gray-900"
											>{company.company_name ?? company.company_number}</span
										>
										{#if company.company_status}
											<span
												class="text-xs {company.company_status.toLowerCase() ===
												'active'
													? 'text-green-600'
													: 'text-gray-400'}"
											>
												{company.company_status}
											</span>
										{/if}
									</div>
									<div class="flex gap-2 text-xs text-gray-400">
										{#if company.company_type}
											<span>{company.company_type}</span>
										{/if}
										{#if company.company_number}
											<span>#{company.company_number}</span>
										{/if}
										{#if company.incorporation_date}
											<span>Est. {formatDate(company.incorporation_date)}</span
											>
										{/if}
									</div>
								</li>
							{/each}
						</ul>
					</div>
				</details>
			{/if}

			{#if hasFoodRatings}
				<details class="group rounded border border-gray-100">
					<summary
						class="flex cursor-pointer items-center gap-2 px-3 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
					>
						<svg
							class="h-4 w-4 transition-transform group-open:rotate-90"
							fill="none"
							viewBox="0 0 24 24"
							stroke="currentColor"
							stroke-width="2"
						>
							<path stroke-linecap="round" stroke-linejoin="round" d="M9 5l7 7-7 7" />
						</svg>
						Food Ratings
						<span class="ml-auto text-xs font-normal text-gray-400"
							>{enrichedAddress.food_ratings?.length}
							{enrichedAddress.food_ratings?.length === 1 ? 'rating' : 'ratings'}</span
						>
					</summary>
					<div class="border-t border-gray-100 px-3 py-2">
						<ul class="space-y-2">
							{#each enrichedAddress.food_ratings ?? [] as rating (rating.id)}
								<li class="text-sm">
									<div class="flex items-center justify-between">
										<span class="font-medium text-gray-900"
											>{rating.business_name ?? 'Unknown business'}</span
										>
										<span
											class="rounded-full px-2 py-0.5 text-xs font-semibold {ratingColor(
												rating.rating_value
											)}"
										>
											{#if rating.rating_value === 'AwaitingInspection'}
												Awaiting
											{:else if rating.rating_value === 'Exempt'}
												Exempt
											{:else if rating.rating_value != null}
												{rating.rating_value}/5
											{:else}
												N/A
											{/if}
										</span>
									</div>
									<div class="flex gap-2 text-xs text-gray-400">
										{#if rating.business_type}
											<span>{rating.business_type}</span>
										{/if}
										{#if rating.rating_date}
											<span>Inspected {formatDate(rating.rating_date)}</span>
										{/if}
									</div>
									{#if rating.scores_hygiene != null || rating.scores_structural != null || rating.scores_management != null}
										<div class="mt-1 flex gap-3 text-xs text-gray-400">
											{#if rating.scores_hygiene != null}
												<span>Hygiene: {rating.scores_hygiene}</span>
											{/if}
											{#if rating.scores_structural != null}
												<span>Structural: {rating.scores_structural}</span>
											{/if}
											{#if rating.scores_management != null}
												<span>Management: {rating.scores_management}</span>
											{/if}
										</div>
									{/if}
								</li>
							{/each}
						</ul>
					</div>
				</details>
			{/if}

			{#if hasVoaRatings}
				<details class="group rounded border border-gray-100">
					<summary
						class="flex cursor-pointer items-center gap-2 px-3 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
					>
						<svg
							class="h-4 w-4 transition-transform group-open:rotate-90"
							fill="none"
							viewBox="0 0 24 24"
							stroke="currentColor"
							stroke-width="2"
						>
							<path stroke-linecap="round" stroke-linejoin="round" d="M9 5l7 7-7 7" />
						</svg>
						VOA Ratings
						<span class="ml-auto text-xs font-normal text-gray-400"
							>{enrichedAddress.voa_ratings?.length}
							{enrichedAddress.voa_ratings?.length === 1 ? 'rating' : 'ratings'}</span
						>
					</summary>
					<div class="border-t border-gray-100 px-3 py-2">
						<ul class="space-y-1.5">
							{#each enrichedAddress.voa_ratings ?? [] as voa (voa.id)}
								<li class="text-sm">
									<div class="flex items-baseline justify-between">
										<span class="font-medium text-gray-900"
											>{voa.description_text ?? 'Commercial property'}</span
										>
										{#if voa.rateable_value != null}
											<span class="text-xs font-semibold text-gray-700"
												>{formatRateableValue(voa.rateable_value)}</span
											>
										{/if}
									</div>
									<div class="flex gap-2 text-xs text-gray-400">
										{#if voa.firm_name}
											<span>{voa.firm_name}</span>
										{/if}
										{#if voa.effective_date}
											<span>Effective {voa.effective_date}</span>
										{/if}
										<span>UARN: {voa.uarn}</span>
									</div>
								</li>
							{/each}
						</ul>
					</div>
				</details>
			{/if}
		</div>
	{/if}
</div>
