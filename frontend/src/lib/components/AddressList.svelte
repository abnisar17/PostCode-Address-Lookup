<script lang="ts">
	import type { PostcodeResponse, AddressResponse } from '$lib/api/types';
	import AddressCard from './AddressCard.svelte';

	let {
		postcode,
		addresses,
		count
	}: {
		postcode: PostcodeResponse;
		addresses: AddressResponse[];
		count: number;
	} = $props();
</script>

<div class="space-y-4">
	<div class="rounded-lg border border-blue-200 bg-blue-50 p-4">
		<div class="flex items-baseline justify-between">
			<h2 class="text-lg font-semibold text-blue-900">{postcode.postcode}</h2>
			<span class="text-sm text-blue-700">{count} {count === 1 ? 'address' : 'addresses'}</span>
		</div>
		{#if postcode.latitude != null && postcode.longitude != null}
			<p class="mt-1 text-sm text-blue-600">
				{postcode.latitude.toFixed(4)}&#176;N, {Math.abs(postcode.longitude).toFixed(
					4
				)}&#176;{postcode.longitude >= 0 ? 'E' : 'W'}
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
	{/if}
</div>
