<script lang="ts">
	import type { AddressListResponse } from '$lib/api/types';
	import AddressCard from './AddressCard.svelte';
	import Pagination from './Pagination.svelte';

	let {
		results,
		onpagechange
	}: {
		results: AddressListResponse;
		onpagechange: (page: number) => void;
	} = $props();

	let totalPages = $derived(Math.ceil(results.total / results.page_size));
	let rangeStart = $derived((results.page - 1) * results.page_size + 1);
	let rangeEnd = $derived(Math.min(results.page * results.page_size, results.total));
</script>

<div class="space-y-4">
	<p class="text-sm text-gray-500">
		Showing {rangeStart}–{rangeEnd} of {results.total} addresses
	</p>

	<div class="space-y-3">
		{#each results.results as address (address.id)}
			<AddressCard {address} />
		{/each}
	</div>

	{#if totalPages > 1}
		<Pagination currentPage={results.page} {totalPages} {onpagechange} />
	{/if}
</div>
