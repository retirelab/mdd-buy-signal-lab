(function(root){
 const isDomestic=s=>s==='069500';
 function merged(rows){return rows.filter(r=>/^\d{4}-\d{2}-\d{2}$/.test(r.date)&&Number.isFinite(Number(r.close))&&Number(r.close)>0).map(r=>({date:r.date,closeUsd:null,closeKrw:Number(r.close)})).sort((a,b)=>a.date.localeCompare(b.date));}
 const api={isDomestic,merged};if(typeof module!=='undefined')module.exports=api;else root.DomesticTools=api;
})(typeof window==='undefined'?globalThis:window);
