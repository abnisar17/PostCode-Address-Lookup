<script lang="ts">
	import type { AddressResponse } from '$lib/api/types';

	let { address }: { address: AddressResponse } = $props();

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
</script>

<div class="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
	<p class="text-gray-900">{formatted || 'No address details'}</p>
	<div class="mt-2 flex flex-wrap items-center gap-3 text-xs text-gray-500">
		{#if confidencePct != null}
			<span>Confidence: {confidencePct}%</span>
		{/if}
		{#if !address.is_complete}
			<span class="rounded-full bg-amber-100 px-2 py-0.5 text-amber-700">Incomplete</span>
		{/if}
		{#if address.latitude != null && address.longitude != null}
			<span>{address.latitude.toFixed(4)}, {address.longitude.toFixed(4)}</span>
		{/if}
	</div>
</div>
