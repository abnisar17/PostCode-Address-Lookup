<script lang="ts">
	import { api } from '$lib/api/client';
	import type { HealthResponse } from '$lib/api/types';

	let health: HealthResponse | null = $state(null);
	let error = $state(false);

	$effect(() => {
		api
			.health()
			.then((data) => {
				health = data;
				error = false;
			})
			.catch(() => {
				error = true;
			});
	});
</script>

<footer class="border-t border-gray-200 bg-gray-50 px-4 py-2 text-sm text-gray-500">
	<div class="mx-auto flex max-w-3xl items-center justify-between">
		{#if health}
			<div class="flex items-center gap-2">
				<span class="inline-block h-2 w-2 rounded-full bg-green-500"></span>
				<span>API connected</span>
			</div>
			<div class="flex flex-wrap gap-4">
				<span>{health.postcode_count.toLocaleString()} postcodes</span>
				<span>{health.address_count.toLocaleString()} addresses</span>
				{#if health.price_paid_count > 0}
					<span>{health.price_paid_count.toLocaleString()} price records</span>
				{/if}
				{#if health.company_count > 0}
					<span>{health.company_count.toLocaleString()} companies</span>
				{/if}
				{#if health.food_rating_count > 0}
					<span>{health.food_rating_count.toLocaleString()} food ratings</span>
				{/if}
			</div>
		{:else if error}
			<div class="flex items-center gap-2">
				<span class="inline-block h-2 w-2 rounded-full bg-red-500"></span>
				<span>API unavailable</span>
			</div>
		{:else}
			<div class="flex items-center gap-2">
				<span class="inline-block h-2 w-2 animate-pulse rounded-full bg-yellow-500"></span>
				<span>Connecting to API...</span>
			</div>
		{/if}
	</div>
</footer>
