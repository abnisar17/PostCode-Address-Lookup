<script lang="ts">
	import type { PostcodeResponse, AddressResponse } from '$lib/api/types';
	import AddressCard from './AddressCard.svelte';

	let {
		postcode,
		addresses,
		count,
		page = 1,
		pageSize = 20,
		total = 0,
		onpagechange
	}: {
		postcode: PostcodeResponse;
		addresses: AddressResponse[];
		count: number;
		page?: number;
		pageSize?: number;
		total?: number;
		onpagechange?: (page: number) => void;
	} = $props();

	let totalPages = $derived(Math.max(1, Math.ceil(total / pageSize)));
</script>

<div class="space-y-4">
	<div class="rounded-lg border border-blue-200 bg-blue-50 p-4">
		<div class="flex items-baseline justify-between">
			<h2 class="text-lg font-semibold text-blue-900">{postcode.postcode}</h2>
			<span class="text-sm text-blue-700">{count} {count === 1 ? 'address' : 'addresses'}</span>
		</div>
		{#if postcode.latitude != null && postcode.longitude != null}
			<p class="mt-1 text-sm text-blue-600">
				{postcode.latitude.toFixed(4)}&deg;N, {Math.abs(postcode.longitude).toFixed(
					4
				)}&deg;{postcode.longitude >= 0 ? 'E' : 'W'}
			</p>
		{/if}
		{#if postcode.local_authority}
			<p class="text-sm text-blue-600">{postcode.local_authority}</p>
		{/if}
		{#if postcode.is_terminated}
			<span class="mt-1 inline-block rounded-full bg-red-100 px-2 py-0.5 text-xs text-red-700"
				>Terminated</span
			>
		{/if}
	</div>

	{#if addresses.length === 0}
		<p class="py-8 text-center text-gray-500">No addresses found for this postcode.</p>
	{:else}
		<div class="space-y-2">
			{#each addresses as address (address.id)}
				<AddressCard {address} />
			{/each}
		</div>

		{#if totalPages > 1 && onpagechange}
			<div class="flex items-center justify-between border-t border-gray-200 pt-4">
				<button
					class="rounded-md border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700 transition-colors hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50"
					disabled={page <= 1}
					onclick={() => onpagechange(page - 1)}
				>
					Previous
				</button>
				<span class="text-sm text-gray-600">
					Page {page} of {totalPages}
				</span>
				<button
					class="rounded-md border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700 transition-colors hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50"
					disabled={page >= totalPages}
					onclick={() => onpagechange(page + 1)}
				>
					Next
				</button>
			</div>
		{/if}
	{/if}
</div>
