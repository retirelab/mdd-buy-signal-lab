(function(root){
 function calculate(observations){
  const valid=new Map();for(const r of observations||[]){if(!/^\d{4}-\d{2}-01$/.test(r.date)||(r.value!==null&&(!Number.isFinite(r.value)||r.value<0||r.value>100)))continue;const d=new Date(r.date+'T00:00:00Z');if(!Number.isFinite(d.getTime())||d.toISOString().slice(0,10)!==r.date)continue;valid.set(r.date,{date:r.date,value:r.value});}
  const rows=[...valid.values()].sort((a,b)=>a.date.localeCompare(b.date));
  const month=s=>Number(s.slice(0,4))*12+Number(s.slice(5,7));
  const result=rows.map((r,i)=>{const window=rows.slice(Math.max(0,i-11),i+1);const mean=window.length===12&&window.every(p=>Number.isFinite(p.value))&&window.every((p,j)=>month(p.date)===month(window[0].date)+j)?window.reduce((s,p)=>s+p.value,0)/12:null;return {...r,mean,gap:mean===null?null:r.value-mean};});
  return {rows:result,latest:result.at(-1)||null};
 }
 const api={calculate};if(typeof module!=='undefined')module.exports=api;else root.UnemploymentTools=api;
})(typeof window==='undefined'?globalThis:window);
