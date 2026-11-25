(function(root){
 // VIX (CBOE 변동성 지수) 일별 종가 요약: 최신값, 전일 대비, 1년 범위 내 위치, 장기 백분위, 구간 해석
 const ZONES=[{max:15,label:'안정',tone:'calm',text:'시장이 평온한 구간입니다.'},{max:20,label:'보통',tone:'normal',text:'평상시 수준의 흔들림입니다.'},{max:30,label:'불안',tone:'uneasy',text:'불안감이 커진 구간입니다.'},{max:Infinity,label:'공포',tone:'fear',text:'시장이 크게 겁먹은 패닉 구간입니다.'}];
 function zone(value){return Number.isFinite(value)?ZONES.find(z=>value<z.max):null;}
 function clean(observations){
  const valid=new Map();
  for(const r of observations||[]){
   if(!r||!/^\d{4}-\d{2}-\d{2}$/.test(r.date))continue;
   const d=new Date(r.date+'T00:00:00Z');if(!Number.isFinite(d.getTime())||d.toISOString().slice(0,10)!==r.date)continue;
   const v=r.value===null?null:Number(r.value);
   if(v!==null&&(!Number.isFinite(v)||v<5||v>150))continue;
   valid.set(r.date,{date:r.date,value:v});
  }
  return [...valid.values()].sort((a,b)=>a.date.localeCompare(b.date));
 }
 function yearBefore(date){const d=new Date(date+'T00:00:00Z');d.setUTCFullYear(d.getUTCFullYear()-1);return d.toISOString().slice(0,10);}
 function calculate(observations){
  const rows=clean(observations);const filled=rows.filter(r=>r.value!==null);
  const latest=filled.at(-1)||null;
  if(!latest)return {rows,latest:null};
  const prev=filled.at(-2)||null;
  const from=yearBefore(latest.date);const year=filled.filter(r=>r.date>from);
  const low=Math.min(...year.map(r=>r.value)),high=Math.max(...year.map(r=>r.value));
  const position=high===low?50:(latest.value-low)/(high-low)*100;
  const below=filled.filter(r=>r.value<latest.value).length,equal=filled.filter(r=>r.value===latest.value).length;
  const percentile=(below+equal/2)/filled.length*100;
  return {rows,latest:{...latest,change:prev?latest.value-prev.value:null,prevDate:prev?.date||null,yearLow:low,yearHigh:high,position,percentile,since:filled[0].date,zone:zone(latest.value)}};
 }
 const api={calculate,zone,ZONES};if(typeof module!=='undefined')module.exports=api;else root.VixTools=api;
})(typeof window==='undefined'?globalThis:window);
