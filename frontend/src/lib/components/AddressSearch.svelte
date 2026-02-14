<script lang="ts">
	import { api, ApiError } from '$lib/api/client';
	import type { AddressListResponse } from '$lib/api/types';
	import SearchResults from './SearchResults.svelte';
	import Spinner from './Spinner.svelte';

	let query = $state('');
	let postcode = $state('');
	let street = $state('');
	let city = $state('');
	let filtersOpen = $state(false);

	let page = $state(1);
	let results: AddressListResponse | null = $state(null);
	let loading = $state(false);
	let error: string | null = $state(null);

	let canSearch = $derived(
		query.trim().length >= 2 ||
			postcode.trim().length > 0 ||
			street.trim().length > 0 ||
			city.trim().length > 0
	);

	function resetPage() {
		page = 1;
	}

	// Debounced search
	$effect(() => {
		const q = query.trim();
		const p = postcode.trim();
		const s = street.trim();
		const c = city.trim();
		const currentPage = page;

		if (!canSearch) {
			results = null;
			error = null;
			return;
		}

		const timer = setTimeout(async () => {
			loading = true;
			error = null;
			try {
				results = await api.searchAddresses({
					q: q || undefined,
					postcode: p || undefined,
					street: s || undefined,
					city: c || undefined,
					page: currentPage
				});
			} catch (err) {
				if (err instanceof ApiError) {
					error = err.detail;
				} else {
					error = 'Search failed. Please try again.';
				}
				results = null;
			} finally {
				loading = false;
			}
		}, 400);

		return () => clearTimeout(timer);
	});

	function handlePageChange(newPage: number) {
		page = newPage;
	}
</script>

<div class="space-y-4">
	<div>
		<input
			bind:value={query}
			oninput={resetPage}
			type="text"
			placeholder="Search addresses (e.g. 10 Downing Street)"
			class="w-full rounded-lg border border-gray-300 px-4 py-3 text-lg shadow-sm transition-colors focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
			aria-label="Search addresses"
		/>
	</div>

	<div>
		<button
			class="text-sm text-gray-500 hover:text-gray-700"
			onclick={() => (filtersOpen = !filtersOpen)}
		>
			{filtersOpen ? 'Hide' : 'Show'} filters
		</button>

		{#if filtersOpen}
			<div class="mt-2 grid grid-cols-1 gap-3 sm:grid-cols-3">
				<input
					bind:value={postcode}
					oninput={resetPage}
					type="text"
					placeholder="Postcode"
					class="rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
				/>
				<input
					bind:value={street}
					oninput={resetPage}
					type="text"
					placeholder="Street"
					class="rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
				/>
				<input
					bind:value={city}
					oninput={resetPage}
					type="text"
					placeholder="City"
					class="rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
				/>
			</div>
		{/if}
	</div>

	{#if loading}
		<div class="flex items-center justify-center py-12 text-blue-500">
			<Spinner />
			<span class="ml-2 text-gray-500">Searching addresses...</span>
		</div>
	{:else if error}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-red-700">
			{error}
		</div>
	{:else if results && results.results.length > 0}
		<SearchResults {results} onpagechange={handlePageChange} />
	{:else if results && results.results.length === 0}
		<div class="py-12 text-center text-gray-400">
			<p class="text-lg">No addresses found</p>
			<p class="mt-1 text-sm">Try adjusting your search terms or filters</p>
		</div>
	{:else}
		<div class="py-12 text-center text-gray-400">
			<p class="text-lg">Search for addresses by name, street, or city</p>
			<p class="mt-1 text-sm">Enter at least 2 characters or use the filters</p>
		</div>
	{/if}
</div>
