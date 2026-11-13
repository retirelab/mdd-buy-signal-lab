(function(root){
 const valid=n=>Number.isFinite(Number(n))&&Number(n)<=-1&&Number(n)>=-100;
 function normalize(s){const map={...(s.targetDdBySymbol||{})};if(!Object.hasOwn(map,s.symbol))map[s.symbol]=valid(s.targetDdPct)?Number(s.targetDdPct):-20;return {...s,targetDdBySymbol:map,targetDdPct:valid(map[s.symbol])?Number(map[s.symbol]):-20};}
 function update(s,change){const prev=normalize(s),next=typeof change==='function'?change(prev):change;const map={...prev.targetDdBySymbol};if(next.symbol===prev.symbol&&valid(next.targetDdPct))map[next.symbol]=Number(next.targetDdPct);return {...next,targetDdBySymbol:map,targetDdPct:valid(map[next.symbol])?Number(map[next.symbol]):-20};}
 const api={normalize,update};if(typeof module!=='undefined')module.exports=api;else root.AssetSettings=api;
})(typeof window==='undefined'?globalThis:window);
