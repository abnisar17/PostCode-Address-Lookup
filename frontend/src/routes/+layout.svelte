<script lang="ts">
	import '../app.css';
	import StatusBar from '$lib/components/StatusBar.svelte';

	let { children } = $props();
</script>

<div class="flex min-h-screen flex-col bg-gray-50">
	<header class="border-b border-gray-200 bg-white px-4 py-4 shadow-sm">
		<div class="mx-auto max-w-3xl">
			<h1 class="text-xl font-semibold text-gray-900">UK Postcode & Address Lookup</h1>
		</div>
	</header>

	<main class="mx-auto w-full max-w-3xl flex-1 px-4 py-6">
		{@render children()}
	</main>

	<StatusBar />
</div>
