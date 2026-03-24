<script lang="ts">
	import '../app.css';
	import StatusBar from '$lib/components/StatusBar.svelte';

	let { children } = $props();
</script>

<div class="flex min-h-screen flex-col bg-gray-50">
	<header class="border-b border-gray-200 bg-white px-3 py-3 shadow-sm sm:px-4 sm:py-4">
		<div class="mx-auto max-w-3xl">
			<h1 class="text-lg font-semibold text-gray-900 sm:text-xl">UK Postcode & Address Lookup</h1>
		</div>
	</header>

	<main class="mx-auto w-full max-w-3xl flex-1 px-3 py-4 pb-20 sm:px-4 sm:py-6 sm:pb-24">
		{@render children()}
	</main>

	<StatusBar />
</div>
