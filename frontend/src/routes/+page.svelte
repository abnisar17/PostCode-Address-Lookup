<script lang="ts">
	import type { PostcodeLookupResponse } from '$lib/api/types';
	import TabSwitcher from '$lib/components/TabSwitcher.svelte';
	import PostcodeSearch from '$lib/components/PostcodeSearch.svelte';
	import AddressList from '$lib/components/AddressList.svelte';
	import AddressSearch from '$lib/components/AddressSearch.svelte';
	import Spinner from '$lib/components/Spinner.svelte';

	const tabs = [
		{ id: 'postcode', label: 'Postcode Lookup' },
		{ id: 'address', label: 'Address Search' }
	];

	let activeTab = $state('postcode');

	let lookupResult: PostcodeLookupResponse | null = $state(null);
	let error: string | null = $state(null);
	let loading = $state(false);

	function handleResult(result: PostcodeLookupResponse) {
		lookupResult = result;
		error = null;
	}

	function handleError(msg: string) {
		error = msg;
		lookupResult = null;
	}

	function handleLoadingChange(isLoading: boolean) {
		loading = isLoading;
		if (isLoading) {
			error = null;
		}
	}
</script>

<div class="space-y-6">
	<TabSwitcher {tabs} active={activeTab} onchange={(id) => (activeTab = id)} />

	{#if activeTab === 'postcode'}
		<PostcodeSearch
			onresult={handleResult}
			onerror={handleError}
			onloadingchange={handleLoadingChange}
		/>

		{#if loading}
			<div class="flex items-center justify-center py-12 text-blue-500">
				<Spinner />
				<span class="ml-2 text-gray-500">Looking up postcode...</span>
			</div>
		{:else if error}
			<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-red-700">
				{error}
			</div>
		{:else if lookupResult}
			<AddressList
				postcode={lookupResult.postcode}
				addresses={lookupResult.addresses}
				count={lookupResult.address_count}
			/>
		{:else}
			<div class="py-12 text-center text-gray-400">
				<p class="text-lg">Start typing a postcode to search</p>
				<p class="mt-1 text-sm">e.g. SW1A 1AA, EC1A 1BB, M1 1AE</p>
			</div>
		{/if}
	{:else if activeTab === 'address'}
		<AddressSearch />
	{/if}
</div>
