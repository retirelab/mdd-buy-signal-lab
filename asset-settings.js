(function(root){
 // 2026-11-25 operator decisions: SPY -20%, QQQ -20%, KODEX 200 -30%, GLD -20% (first-tranche thresholds).
 const DEFAULT_TARGETS=Object.freeze({SPY:-20,QQQ:-20,'069500':-30,GLD:-20});
 const PRESET_VERSION='2026-11-25b';
 const valid=n=>Number.isFinite(Number(n))&&Number(n)<=-1&&Number(n)>=-100;
 const fallback=sym=>Object.hasOwn(DEFAULT_TARGETS,sym)?DEFAULT_TARGETS[sym]:-20;
 function normalize(s){
  let map={...(s.targetDdBySymbol||{})};
  if(s.thresholdPreset!==PRESET_VERSION){
   // Apply the new standard once; later manual edits are kept.
   map={...map,...DEFAULT_TARGETS};
  }else if(!Object.hasOwn(map,s.symbol)){
   map[s.symbol]=fallback(s.symbol);
  }
  return {...s,thresholdPreset:PRESET_VERSION,targetDdBySymbol:map,targetDdPct:valid(map[s.symbol])?Number(map[s.symbol]):fallback(s.symbol)};
 }
 function update(s,change){
  const prev=normalize(s),next=typeof change==='function'?change(prev):change;
  const map={...prev.targetDdBySymbol};
  if(next.symbol===prev.symbol&&valid(next.targetDdPct))map[next.symbol]=Number(next.targetDdPct);
  return {...next,thresholdPreset:PRESET_VERSION,targetDdBySymbol:map,targetDdPct:valid(map[next.symbol])?Number(map[next.symbol]):fallback(next.symbol)};
 }
 const api={normalize,update,DEFAULT_TARGETS,PRESET_VERSION};if(typeof module!=='undefined')module.exports=api;else root.AssetSettings=api;
})(typeof window==='undefined'?globalThis:window);
