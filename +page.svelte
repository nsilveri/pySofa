<script lang="ts">
  import { onMount } from 'svelte';
  import { page } from '$app/stores';

  type GraphLoadState =
    | { status: 'idle' }
    | { status: 'loading' }
    | { status: 'ready'; points: Array<{ minute: number; value: number }> }
    | { status: 'error'; message: string };

  type Candle = { minuteStart: number; open: number; high: number; low: number; close: number };
  type CandlePaths = { wicks: string; bodies: string };

  let eventId: number | null = null;
  $: eventId = Number($page.params.eventId);
  $: if (!Number.isFinite(eventId)) eventId = null;

  let eventInfo: any | null = null;
  let eventInfoError = '';
  let homeTeamId: number | null = null;
  let awayTeamId: number | null = null;

  let graphState: GraphLoadState = { status: 'idle' };
  let candlePaths: CandlePaths | null = null;
  let candlesForHover: Candle[] = [];

  type GoalMarker = { minute: number; teamSide: 'home' | 'away' | 'unknown' };
  let goalMarkers: GoalMarker[] = [];
  let goalError = '';

  let hoverLine: { minute: number; value: number } | null = null;
  let hoverCandle: { minute: number; value: number } | null = null;

  const CHART_WIDTH = 920;
  const CHART_HEIGHT = 260;
  const CHART_PADDING = 10;
  const CANDLE_BUCKET_MINUTES = 1;

  const sofascoreHeaders: Record<string, string> = {
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0',
    Referer: 'https://www.sofascore.com/',
    'sec-ch-ua': '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin'
  };

  function toNumber(value: unknown): number | null {
    const numberValue = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
    return Number.isFinite(numberValue) ? numberValue : null;
  }

  function extractGraphPoints(payload: any): Array<{ minute: number; value: number }> {
    const candidates: unknown[] = [
      payload?.graphPoints,
      payload?.graph?.graphPoints,
      payload?.data?.graphPoints,
      payload?.points,
      payload?.graph?.points
    ].filter(Boolean);

    for (const candidate of candidates) {
      if (!Array.isArray(candidate)) continue;
      const points: Array<{ minute: number; value: number }> = [];
      for (const rawPoint of candidate as any[]) {
        const minute = toNumber(rawPoint?.minute ?? rawPoint?.x ?? rawPoint?.time ?? rawPoint?.t);
        const value = toNumber(rawPoint?.value ?? rawPoint?.y ?? rawPoint?.v);
        if (minute === null || value === null) continue;
        points.push({ minute, value });
      }
      if (points.length > 1) return points;
    }

    return [];
  }

  function buildCandlesFromPoints(pointsRaw: Array<{ minute: number; value: number }>, bucketMinutes: number): Candle[] {
    if (!Array.isArray(pointsRaw) || pointsRaw.length === 0) return [];
    const bucket = Math.max(1, Math.floor(bucketMinutes));

    const sorted = [...pointsRaw]
      .filter((p) => Number.isFinite(p.minute) && Number.isFinite(p.value))
      .sort((a, b) => a.minute - b.minute);

    const byBucket = new Map<number, Array<{ minute: number; value: number }>>();
    for (const p of sorted) {
      const key = Math.floor(p.minute / bucket);
      const list = byBucket.get(key);
      if (list) list.push(p);
      else byBucket.set(key, [p]);
    }

    const keys = [...byBucket.keys()].sort((a, b) => a - b);
    const candles: Candle[] = [];
    for (const key of keys) {
      const list = byBucket.get(key);
      if (!list || list.length === 0) continue;
      const open = list[0].value;
      const close = list[list.length - 1].value;
      let high = -Infinity;
      let low = Infinity;
      for (const p of list) {
        if (p.value > high) high = p.value;
        if (p.value < low) low = p.value;
      }
      if (!Number.isFinite(high) || !Number.isFinite(low)) continue;
      candles.push({ minuteStart: key * bucket, open, high, low, close });
    }
    return candles;
  }

  function candlestickPathsFromCandles(candlesRaw: Candle[]): CandlePaths | null {
    if (!Array.isArray(candlesRaw) || candlesRaw.length === 0) return null;
    const candles = [...candlesRaw].sort((a, b) => a.minuteStart - b.minuteStart);

    const maxAbs = Math.max(
      1,
      ...candles.flatMap((c) => [Math.abs(c.open), Math.abs(c.high), Math.abs(c.low), Math.abs(c.close)])
    );

    const innerW = CHART_WIDTH - CHART_PADDING * 2;
    const step = innerW / Math.max(1, candles.length);
    const candleW = Math.max(2, step * 0.7);

    const toXCenter = (index: number) => CHART_PADDING + (index + 0.5) * step;
    const toY = (value: number) =>
      CHART_HEIGHT / 2 - (value / maxAbs) * (CHART_HEIGHT / 2 - CHART_PADDING);

    let wicks = '';
    let bodies = '';

    candles.forEach((c, i) => {
      const x = toXCenter(i);
      const yHigh = toY(c.high);
      const yLow = toY(c.low);
      wicks += `M ${x.toFixed(2)} ${yHigh.toFixed(2)} L ${x.toFixed(2)} ${yLow.toFixed(2)} `;

      const yOpen = toY(c.open);
      const yClose = toY(c.close);
      let top = Math.min(yOpen, yClose);
      let bottom = Math.max(yOpen, yClose);
      if (Math.abs(bottom - top) < 1) bottom = top + 1;

      const left = x - candleW / 2;
      const right = x + candleW / 2;
      bodies +=
        `M ${left.toFixed(2)} ${top.toFixed(2)} ` +
        `L ${right.toFixed(2)} ${top.toFixed(2)} ` +
        `L ${right.toFixed(2)} ${bottom.toFixed(2)} ` +
        `L ${left.toFixed(2)} ${bottom.toFixed(2)} Z `;
    });

    return { wicks: wicks.trim(), bodies: bodies.trim() };
  }

  function linePathFromPoints(pointsRaw: Array<{ minute: number; value: number }>): string | null {
    if (!Array.isArray(pointsRaw) || pointsRaw.length < 2) return null;

    const points = [...pointsRaw].sort((a, b) => a.minute - b.minute);
    const minutes = points.map((p) => p.minute);
    const values = points.map((p) => p.value);

    const minMinute = Math.min(...minutes);
    const maxMinute = Math.max(...minutes);
    const minuteSpan = Math.max(1, maxMinute - minMinute);
    const maxAbs = Math.max(1, ...values.map((v) => Math.abs(v)));

    const toX = (minute: number) =>
      CHART_PADDING + ((minute - minMinute) / minuteSpan) * (CHART_WIDTH - CHART_PADDING * 2);

    const toY = (value: number) =>
      CHART_HEIGHT / 2 - (value / maxAbs) * (CHART_HEIGHT / 2 - CHART_PADDING);

    return (
      'M ' +
      points
        .map((p) => `${toX(p.minute).toFixed(2)} ${toY(p.value).toFixed(2)}`)
        .join(' L ')
    );
  }

  function clamp(n: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, n));
  }

  function computeChartMeta(pointsRaw: Array<{ minute: number; value: number }>) {
    const points = [...pointsRaw].sort((a, b) => a.minute - b.minute);
    const minutes = points.map((p) => p.minute);
    const values = points.map((p) => p.value);

    const minMinute = Math.min(...minutes);
    const maxMinute = Math.max(...minutes);
    const minuteSpan = Math.max(1, maxMinute - minMinute);
    const maxAbs = Math.max(1, ...values.map((v) => Math.abs(v)));

    return { minMinute, maxMinute, minuteSpan, maxAbs };
  }

  function buildXTicks(meta: { minMinute: number; maxMinute: number }, count = 7): number[] {
    void count;
    const minM = Math.floor(meta.minMinute);
    const maxM = Math.ceil(meta.maxMinute);
    if (maxM <= minM) return [minM];

    const step = 5;
    const first = Math.ceil(minM / step) * step;
    const last = Math.floor(maxM / step) * step;
    const ticks: number[] = [];
    for (let m = first; m <= last; m += step) ticks.push(m);
    if (ticks.length === 0) ticks.push(first);
    return ticks;
  }

  function buildYTicks(meta: { maxAbs: number }): number[] {
    const maxAbs = Math.max(1, meta.maxAbs);
    const raw = [-maxAbs, -maxAbs / 2, 0, maxAbs / 2, maxAbs];
    return raw.map((v) => {
      const rounded = Math.round(v * 100) / 100;
      return Object.is(rounded, -0) ? 0 : rounded;
    });
  }

  function xFromMinute(meta: { minMinute: number; minuteSpan: number }, minute: number): number {
    const x =
      CHART_PADDING +
      ((minute - meta.minMinute) / meta.minuteSpan) * (CHART_WIDTH - CHART_PADDING * 2);
    return clamp(x, CHART_PADDING, CHART_WIDTH - CHART_PADDING);
  }

  function yFromValue(meta: { maxAbs: number }, value: number): number {
    const y = CHART_HEIGHT / 2 - (value / meta.maxAbs) * (CHART_HEIGHT / 2 - CHART_PADDING);
    return clamp(y, CHART_PADDING, CHART_HEIGHT - CHART_PADDING);
  }

  function formatNumber(value: number): string {
    if (!Number.isFinite(value)) return 'N/D';
    const rounded = Math.round(value * 100) / 100;
    return Number.isInteger(rounded) ? String(rounded) : rounded.toFixed(2);
  }

  function labelY(y: number): number {
    return clamp(y - 6, 10, CHART_HEIGHT - 10);
  }

  function onLineMouseMove(e: MouseEvent) {
    if (graphState.status !== 'ready') return;
    const svg = e.currentTarget as SVGSVGElement | null;
    if (!svg) return;

    const meta = computeChartMeta(graphState.points);
    const rect = svg.getBoundingClientRect();
    if (rect.width <= 0) return;

    const xPx = ((e.clientX - rect.left) / rect.width) * CHART_WIDTH;
    const innerW = CHART_WIDTH - CHART_PADDING * 2;
    const xInner = clamp(xPx - CHART_PADDING, 0, innerW);
    const minute = meta.minMinute + (xInner / innerW) * meta.minuteSpan;

    let best = graphState.points[0];
    let bestDist = Infinity;
    for (const p of graphState.points) {
      const d = Math.abs(p.minute - minute);
      if (d < bestDist) {
        bestDist = d;
        best = p;
      }
    }
    hoverLine = { minute: Math.round(best.minute), value: best.value };
  }

  function onLineMouseLeave() {
    hoverLine = null;
  }

  function coloredLinePaths(pointsRaw: Array<{ minute: number; value: number }>): { pos: string | null; neg: string | null } {
    if (!Array.isArray(pointsRaw) || pointsRaw.length < 2) return { pos: null, neg: null };
    const points = [...pointsRaw].sort((a, b) => a.minute - b.minute);
    const meta = computeChartMeta(points);

    const yZero = yFromValue(meta, 0);

    let posD = '';
    let negD = '';
    let posStarted = false;
    let negStarted = false;

    const add = (target: 'pos' | 'neg', x: number, y: number, move: boolean) => {
      const cmd = `${move ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)} `;
      if (target === 'pos') {
        posD += cmd;
        posStarted = true;
      } else {
        negD += cmd;
        negStarted = true;
      }
    };

    function sign(v: number): 'pos' | 'neg' {
      return v >= 0 ? 'pos' : 'neg';
    }

    let prev = points[0];
    let prevSign = sign(prev.value);
    const x0 = xFromMinute(meta, prev.minute);
    const y0 = yFromValue(meta, prev.value);
    add(prevSign, x0, y0, true);

    for (let i = 1; i < points.length; i++) {
      const curr = points[i];
      const currSign = sign(curr.value);

      const x1 = xFromMinute(meta, curr.minute);
      const y1 = yFromValue(meta, curr.value);

      if (currSign === prevSign) {
        add(currSign, x1, y1, false);
        prev = curr;
        prevSign = currSign;
        continue;
      }

      // Crosses 0: split segment at intersection.
      const v0 = prev.value;
      const v1 = curr.value;
      const denom = v0 - v1;
      const t = denom === 0 ? 0 : v0 / denom; // in [0,1] when truly crossing
      const minuteCross = prev.minute + (curr.minute - prev.minute) * t;
      const xCross = xFromMinute(meta, minuteCross);

      // Finish previous color at 0
      add(prevSign, xCross, yZero, false);

      // Start new color from 0
      add(currSign, xCross, yZero, true);
      add(currSign, x1, y1, false);

      prev = curr;
      prevSign = currSign;
    }

    return {
      pos: posD.trim().length > 0 ? posD.trim() : null,
      neg: negD.trim().length > 0 ? negD.trim() : null
    };
  }

  function onCandleMouseMove(e: MouseEvent) {
    if (!candlesForHover || candlesForHover.length === 0) return;
    const svg = e.currentTarget as SVGSVGElement | null;
    if (!svg) return;
    const rect = svg.getBoundingClientRect();
    if (rect.width <= 0) return;

    const xPx = ((e.clientX - rect.left) / rect.width) * CHART_WIDTH;

    const innerW = CHART_WIDTH - CHART_PADDING * 2;
    const step = innerW / Math.max(1, candlesForHover.length);
    const idx = clamp(Math.floor((xPx - CHART_PADDING) / step), 0, candlesForHover.length - 1);
    const candle = candlesForHover[idx];
    hoverCandle = { minute: Math.round(candle.minuteStart), value: candle.close };
  }

  function onCandleMouseLeave() {
    hoverCandle = null;
  }

  async function loadEventInfo(id: number) {
    eventInfo = null;
    eventInfoError = '';
    homeTeamId = null;
    awayTeamId = null;
    try {
      const url = `https://www.sofascore.com/api/v1/event/${id}`;
      const response = await fetch(url, { method: 'GET', headers: sofascoreHeaders });
      if (!response.ok) {
        eventInfoError = `${response.status} ${response.statusText}`;
        return;
      }
      const payload = await response.json();
      eventInfo = payload?.event ?? payload;
      homeTeamId = toNumber(eventInfo?.homeTeam?.id);
      awayTeamId = toNumber(eventInfo?.awayTeam?.id);
    } catch (e: any) {
      eventInfoError = e?.message ?? 'Errore sconosciuto';
    }
  }

  function parseGoalMarkers(payload: any, teamIds?: { homeTeamId: number | null; awayTeamId: number | null }): GoalMarker[] {
    const incidents = payload?.incidents;
    if (!Array.isArray(incidents)) return [];

    const out: GoalMarker[] = [];
    for (const inc of incidents) {
      const incidentType = String(inc?.incidentType ?? inc?.type ?? '').toLowerCase();
      if (!incidentType.includes('goal')) continue;

      const time = toNumber(inc?.time);
      if (time === null) continue;
      const added = toNumber(inc?.addedTime) ?? 0;
      const minute = Math.round(time + added);

      const sideRaw = String(inc?.teamSide ?? '').toLowerCase();
      let teamSide: GoalMarker['teamSide'] = sideRaw === 'home' ? 'home' : sideRaw === 'away' ? 'away' : 'unknown';

      // Fallback: some payloads don't include teamSide; infer it from team.id.
      if (teamSide === 'unknown') {
        const incidentTeamId = toNumber(inc?.team?.id ?? inc?.teamId);
        if (incidentTeamId !== null) {
          const homeId = teamIds?.homeTeamId ?? null;
          const awayId = teamIds?.awayTeamId ?? null;
          if (homeId !== null && incidentTeamId === homeId) teamSide = 'home';
          else if (awayId !== null && incidentTeamId === awayId) teamSide = 'away';
        }
      }
      out.push({ minute, teamSide });
    }

    // Keep deterministic order.
    out.sort((a, b) => a.minute - b.minute || a.teamSide.localeCompare(b.teamSide));
    return out;
  }

  async function loadGoalMarkers(id: number) {
    goalMarkers = [];
    goalError = '';
    try {
      const url = `https://www.sofascore.com/api/v1/event/${id}/incidents`;
      const response = await fetch(url, { method: 'GET', headers: sofascoreHeaders });
      if (!response.ok) {
        goalError = `${response.status} ${response.statusText}`;
        return;
      }
      const payload = await response.json();
      goalMarkers = parseGoalMarkers(payload, { homeTeamId, awayTeamId });
    } catch (e: any) {
      goalError = e?.message ?? 'Errore sconosciuto';
    }
  }

  async function loadGraph(id: number) {
    graphState = { status: 'loading' };
    candlePaths = null;
    candlesForHover = [];
    try {
      const url = `https://www.sofascore.com/api/v1/event/${id}/graph`;
      const response = await fetch(url, { method: 'GET', headers: sofascoreHeaders });
      if (!response.ok) {
        graphState = { status: 'error', message: `${response.status} ${response.statusText}` };
        return;
      }
      const payload = await response.json();
      const points = extractGraphPoints(payload);
      graphState = { status: 'ready', points };

      const candles = buildCandlesFromPoints(points, CANDLE_BUCKET_MINUTES);
      candlePaths = candlestickPathsFromCandles(candles);
      candlesForHover = candles;
    } catch (e: any) {
      graphState = { status: 'error', message: e?.message ?? 'Errore sconosciuto' };
    }
  }

  let lastLoadedId: number | null = null;

  async function loadAllIfNeeded() {
    if (eventId === null) return;
    if (lastLoadedId === eventId) return;
    lastLoadedId = eventId;

    graphState = { status: 'idle' };
    candlePaths = null;
    eventInfo = null;
    eventInfoError = '';
    goalMarkers = [];
    goalError = '';

    // Load event first so we can classify goals as home/away using team IDs.
    await loadEventInfo(eventId);
    await Promise.all([loadGraph(eventId), loadGoalMarkers(eventId)]);
  }

  onMount(() => {
    void loadAllIfNeeded();
  });

  $: void loadAllIfNeeded();

  $: linePath =
    graphState.status === 'ready' ? linePathFromPoints(graphState.points) : null;

  $: chartMeta = graphState.status === 'ready' ? computeChartMeta(graphState.points) : null;
  $: xTicks = chartMeta ? buildXTicks(chartMeta, 7) : [];
  $: yTicks = chartMeta ? buildYTicks(chartMeta) : [];

  $: coloredPaths =
    graphState.status === 'ready' ? coloredLinePaths(graphState.points) : { pos: null, neg: null };

  $: goalGroups = (() => {
    const byMinute = new Map<number, GoalMarker[]>();
    for (const g of goalMarkers) {
      const list = byMinute.get(g.minute);
      if (list) list.push(g);
      else byMinute.set(g.minute, [g]);
    }
    const minutes = [...byMinute.keys()].sort((a, b) => a - b);
    return minutes.map((minute) => ({ minute, items: byMinute.get(minute) ?? [] }));
  })();
</script>

<div class="px-5 flex-1 flex flex-col overflow-hidden">
  <div class="pt-6 pb-4 flex items-center justify-between gap-4">
    <div class="min-w-0">
      <h2 class="text-gray-600 dark:text-gray-300 text-2xl truncate">
        Grafici partita
      </h2>
      <div class="text-sm text-gray-500 dark:text-gray-400">
        {#if eventId !== null}
          ID: {eventId}
        {:else}
          ID non valido
        {/if}
      </div>
    </div>
    <div class="flex items-center gap-3">
      <a
        href="/"
        class="px-3 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-500"
      >
        Torna alla lista
      </a>
      <a
        href="/favorites"
        class="px-3 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-500"
      >
        Preferiti
      </a>
    </div>
  </div>

  {#if eventInfoError}
    <p class="pb-3 text-red-500">Dettagli partita non disponibili: {eventInfoError}</p>
  {/if}

  {#if goalError}
    <p class="pb-3 text-red-500">Gol non disponibili: {goalError}</p>
  {/if}

  {#if eventInfo}
    <div class="pb-4">
      <div class="text-gray-700 dark:text-gray-200 font-semibold">
        {eventInfo?.homeTeam?.name} {eventInfo?.homeScore?.current} - {eventInfo?.awayScore?.current} {eventInfo?.awayTeam?.name}
      </div>
      <div class="text-sm text-gray-600 dark:text-gray-400">
        {eventInfo?.tournament?.name} - {eventInfo?.status?.description}
      </div>
    </div>
  {/if}

  <div class="flex-1 overflow-y-auto pb-5">
    <div class="border border-gray-300 dark:border-gray-600 rounded-lg p-4">
      <div class="flex items-center justify-between gap-4">
        <h3 class="text-gray-700 dark:text-gray-200 font-semibold">Grafico (linea)</h3>
        {#if graphState.status === 'loading'}
          <span class="text-sm text-gray-500 dark:text-gray-400">Caricamento…</span>
        {:else if graphState.status === 'error'}
          <span class="text-sm text-red-500">{graphState.message}</span>
        {:else if graphState.status === 'ready'}
          <span class="text-sm text-gray-500 dark:text-gray-400">Punti: {graphState.points.length}</span>
        {/if}
      </div>

      {#if graphState.status === 'ready' && linePath}
        <div class="mt-3 w-full overflow-x-auto">
          <svg
            class="w-full"
            style="min-width: 920px; height: 260px"
            viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT}`}
            preserveAspectRatio="none"
            aria-label="grafico linea"
            role="img"
            on:mousemove={onLineMouseMove}
            on:mouseleave={onLineMouseLeave}
          >
            {#if chartMeta}
              {#each yTicks as v}
                <line
                  x1="0"
                  y1={yFromValue(chartMeta, v)}
                  x2={CHART_WIDTH}
                  y2={yFromValue(chartMeta, v)}
                  class="stroke-gray-200 dark:stroke-gray-700"
                />
                <text
                  x="6"
                  y={yFromValue(chartMeta, v) - 4}
                  class="fill-gray-500 dark:fill-gray-400"
                  font-size="10"
                >
                  {formatNumber(v)}
                </text>
              {/each}
              {#each xTicks as m}
                <line
                  x1={xFromMinute(chartMeta, m)}
                  y1="0"
                  x2={xFromMinute(chartMeta, m)}
                  y2={CHART_HEIGHT}
                  class="stroke-gray-200 dark:stroke-gray-700"
                />
                <text
                  x={xFromMinute(chartMeta, m)}
                  y={CHART_HEIGHT - 6}
                  class="fill-gray-500 dark:fill-gray-400"
                  font-size="10"
                  text-anchor="middle"
                >
                  {m}'
                </text>
              {/each}

              {#each goalGroups as g (g.minute)}
                {@const x = xFromMinute(chartMeta, g.minute)}
                {@const homeItems = g.items.filter((it) => it.teamSide === 'home')}
                {@const awayItems = g.items.filter((it) => it.teamSide === 'away')}
                {@const unknownItems = g.items.filter((it) => it.teamSide !== 'home' && it.teamSide !== 'away')}

                {#each homeItems as item, i (g.minute + ':home:' + i)}
                  {@const y = CHART_HEIGHT / 2 - 18 - i * 16}
                  <text x={x} y={y} class="fill-blue-600 dark:fill-blue-300" font-size="14" text-anchor="middle" aria-label="gol casa">
                    <tspan>⚽</tspan>
                    <tspan dx="4" font-size="10">{g.minute}'</tspan>
                  </text>
                {/each}

                {#each awayItems as item, i (g.minute + ':away:' + i)}
                  {@const y = CHART_HEIGHT / 2 + 22 + i * 16}
                  <text x={x} y={y} class="fill-purple-600 dark:fill-purple-300" font-size="14" text-anchor="middle" aria-label="gol trasferta">
                    <tspan>⚽</tspan>
                    <tspan dx="4" font-size="10">{g.minute}'</tspan>
                  </text>
                {/each}

                {#each unknownItems as item, i (g.minute + ':unknown:' + i)}
                  {@const y = CHART_HEIGHT / 2 - 2 - i * 16}
                  <text x={x} y={y} class="fill-gray-600 dark:fill-gray-300" font-size="14" text-anchor="middle" aria-label="gol">
                    <tspan>⚽</tspan>
                    <tspan dx="4" font-size="10">{g.minute}'</tspan>
                  </text>
                {/each}
              {/each}
            {/if}
            <line
              x1="0"
              y1={CHART_HEIGHT / 2}
              x2={CHART_WIDTH}
              y2={CHART_HEIGHT / 2}
              class="stroke-gray-300 dark:stroke-gray-600"
            />
            {#if coloredPaths.neg}
              <path d={coloredPaths.neg} fill="none" class="stroke-red-500 dark:stroke-red-400" stroke-width="2" />
            {/if}
            {#if coloredPaths.pos}
              <path d={coloredPaths.pos} fill="none" class="stroke-green-500 dark:stroke-green-400" stroke-width="2" />
            {/if}

            {#if chartMeta}
              {#each graphState.points as p (p.minute)}
                {@const x = xFromMinute(chartMeta, Math.round(p.minute))}
                {@const y = yFromValue(chartMeta, p.value)}
                <circle
                  cx={x}
                  cy={y}
                  r="2.5"
                  class={p.value >= 0 ? 'fill-green-500 dark:fill-green-400' : 'fill-red-500 dark:fill-red-400'}
                />
                <text
                  x={x}
                  y={labelY(y)}
                  class={p.value >= 0 ? 'fill-green-600 dark:fill-green-300' : 'fill-red-600 dark:fill-red-300'}
                  font-size="9"
                  text-anchor="middle"
                >
                  {formatNumber(p.value)}
                </text>
              {/each}
            {/if}
          </svg>
        </div>
        {#if hoverLine}
          <div class="mt-2 text-sm text-gray-600 dark:text-gray-400">
            Minuto: {hoverLine.minute} — Valore: {formatNumber(hoverLine.value)}
          </div>
        {/if}
      {:else if graphState.status === 'ready'}
        <div class="mt-3 text-sm text-gray-500 dark:text-gray-400">Nessun dato grafico disponibile.</div>
      {/if}
    </div>

    <div class="mt-4 border border-gray-300 dark:border-gray-600 rounded-lg p-4">
      <div class="flex items-center justify-between gap-4">
        <h3 class="text-gray-700 dark:text-gray-200 font-semibold">Grafico (candele)</h3>
        {#if graphState.status === 'loading'}
          <span class="text-sm text-gray-500 dark:text-gray-400">Caricamento…</span>
        {:else if graphState.status === 'error'}
          <span class="text-sm text-red-500">{graphState.message}</span>
        {/if}
      </div>

      {#if graphState.status === 'ready' && candlePaths}
        <div class="mt-3 w-full overflow-x-auto">
          <svg
            class="w-full"
            style="min-width: 920px; height: 260px"
            viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT}`}
            preserveAspectRatio="none"
            aria-label="grafico candele"
            role="img"
            on:mousemove={onCandleMouseMove}
            on:mouseleave={onCandleMouseLeave}
          >
            {#if chartMeta}
              {#each yTicks as v}
                <line
                  x1="0"
                  y1={yFromValue(chartMeta, v)}
                  x2={CHART_WIDTH}
                  y2={yFromValue(chartMeta, v)}
                  class="stroke-gray-200 dark:stroke-gray-700"
                />
                <text
                  x="6"
                  y={yFromValue(chartMeta, v) - 4}
                  class="fill-gray-500 dark:fill-gray-400"
                  font-size="10"
                >
                  {formatNumber(v)}
                </text>
              {/each}
              {#each xTicks as m}
                <line
                  x1={xFromMinute(chartMeta, m)}
                  y1="0"
                  x2={xFromMinute(chartMeta, m)}
                  y2={CHART_HEIGHT}
                  class="stroke-gray-200 dark:stroke-gray-700"
                />
                <text
                  x={xFromMinute(chartMeta, m)}
                  y={CHART_HEIGHT - 6}
                  class="fill-gray-500 dark:fill-gray-400"
                  font-size="10"
                  text-anchor="middle"
                >
                  {m}'
                </text>
              {/each}

              {#each goalGroups as g (g.minute)}
                {@const x = xFromMinute(chartMeta, g.minute)}
                {@const homeItems = g.items.filter((it) => it.teamSide === 'home')}
                {@const awayItems = g.items.filter((it) => it.teamSide === 'away')}
                {@const unknownItems = g.items.filter((it) => it.teamSide !== 'home' && it.teamSide !== 'away')}

                {#each homeItems as item, i (g.minute + ':home:' + i)}
                  {@const y = CHART_HEIGHT / 2 - 18 - i * 16}
                  <text x={x} y={y} class="fill-blue-600 dark:fill-blue-300" font-size="14" text-anchor="middle" aria-label="gol casa">
                    <tspan>⚽</tspan>
                    <tspan dx="4" font-size="10">{g.minute}'</tspan>
                  </text>
                {/each}

                {#each awayItems as item, i (g.minute + ':away:' + i)}
                  {@const y = CHART_HEIGHT / 2 + 22 + i * 16}
                  <text x={x} y={y} class="fill-purple-600 dark:fill-purple-300" font-size="14" text-anchor="middle" aria-label="gol trasferta">
                    <tspan>⚽</tspan>
                    <tspan dx="4" font-size="10">{g.minute}'</tspan>
                  </text>
                {/each}

                {#each unknownItems as item, i (g.minute + ':unknown:' + i)}
                  {@const y = CHART_HEIGHT / 2 - 2 - i * 16}
                  <text x={x} y={y} class="fill-gray-600 dark:fill-gray-300" font-size="14" text-anchor="middle" aria-label="gol">
                    <tspan>⚽</tspan>
                    <tspan dx="4" font-size="10">{g.minute}'</tspan>
                  </text>
                {/each}
              {/each}
            {/if}
            <line
              x1="0"
              y1={CHART_HEIGHT / 2}
              x2={CHART_WIDTH}
              y2={CHART_HEIGHT / 2}
              class="stroke-gray-300 dark:stroke-gray-600"
            />
            <path d={candlePaths.wicks} fill="none" class="stroke-gray-700 dark:stroke-gray-200" stroke-width="1.6" />
            <path d={candlePaths.bodies} fill="none" class="stroke-gray-700 dark:stroke-gray-200" stroke-width="1.6" />
          </svg>
        </div>
        {#if hoverCandle}
          <div class="mt-2 text-sm text-gray-600 dark:text-gray-400">
            Minuto: {hoverCandle.minute} — Valore: {formatNumber(hoverCandle.value)}
          </div>
        {/if}
      {:else if graphState.status === 'ready'}
        <div class="mt-3 text-sm text-gray-500 dark:text-gray-400">Nessun dato grafico disponibile.</div>
      {/if}
    </div>
  </div>
</div>
