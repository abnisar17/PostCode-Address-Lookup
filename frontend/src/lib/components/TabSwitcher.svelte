<script lang="ts">
	let {
		tabs,
		active,
		onchange
	}: {
		tabs: { id: string; label: string }[];
		active: string;
		onchange: (id: string) => void;
	} = $props();
</script>

<div role="tablist" class="flex border-b border-gray-200">
	{#each tabs as tab (tab.id)}
		<button
			role="tab"
			aria-selected={tab.id === active}
			class="px-4 py-2.5 text-sm font-medium transition-colors {tab.id === active
				? 'border-b-2 border-blue-500 text-blue-600'
				: 'text-gray-500 hover:text-gray-700'}"
			onclick={() => onchange(tab.id)}
		>
			{tab.label}
		</button>
	{/each}
</div>
