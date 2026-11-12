(function(root){
  'use strict';
  const EMPTY=[];
  function isoDay(time){
    if(typeof time==='string') {const date=new Date(time+'T00:00:00Z');return /^\d{4}-\d{2}-\d{2}$/.test(time)&&Number.isFinite(date.getTime())&&date.toISOString().slice(0,10)===time?time:null;}
    if(typeof time==='number') {const date=new Date(time*1000);return Number.isFinite(date.getTime())?date.toISOString().slice(0,10):null;}
    if(time && Number.isInteger(time.year)&&Number.isInteger(time.month)&&Number.isInteger(time.day)) return isoDay(`${time.year}-${String(time.month).padStart(2,'0')}-${String(time.day).padStart(2,'0')}`);
    return null;
  }
  function prepareSeries(rows,definitions){
    const byDate=new Map();
    for(const row of rows||[]) if(isoDay(row.date)) byDate.set(row.date,row);
    const ordered=[...byDate.values()].sort((a,b)=>a.date.localeCompare(b.date));
    return definitions.map(def=>({...def,data:ordered.map(row=>Number.isFinite(row[def.key])?{time:row.date,value:row[def.key]}:{time:row.date})}));
  }
  function markersForSeries(series,markers){
    const dates=new Set(series.data.filter(r=>Number.isFinite(r.value)).map(r=>r.time));
    return markers.filter(m=>m.seriesId===series.id&&dates.has(m.date)).map(m=>({time:m.date,position:'inBar',shape:'circle',color:m.color||series.color,text:m.text||'',size:m.size||1})).sort((a,b)=>a.time.localeCompare(b.time));
  }
  function formatValue(value,kind){
    if(!Number.isFinite(value))return '—';
    if(kind==='percent')return (value*100).toFixed(2)+'%';
    return (kind==='usd'?'$':'')+value.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
  }
  function includeReferenceRange(info,references){
    if(!info?.priceRange)return info;
    const prices=references.map(r=>r.value).filter(Number.isFinite);
    return {...info,priceRange:{minValue:Math.min(info.priceRange.minValue,...prices),maxValue:Math.max(info.priceRange.maxValue,...prices)}};
  }
  function Plot(props){
    const React=root.React,h=React.createElement;
    const {useState,useRef,useEffect}=React;
    const host=useRef(null),engine=useRef(null),latest=useRef(props),syncing=useRef(false),previousDate=useRef(props.selectedDate),previousSelectionKey=useRef(props.selectionKey),pinned=useRef(false);
    latest.current=props;
    const [failed,setFailed]=useState(false),[hover,setHover]=useState(null),[hidden,setHidden]=useState({}),[log,setLog]=useState(false),[visibleRange,setVisibleRange]=useState(null),[inspecting,setInspecting]=useState(false),[showMarkers,setShowMarkers]=useState(!!props.defaultShowMarkers);
    const series=props.series||EMPTY,markers=props.markers||EMPTY,references=props.references||EMPTY;
    const kind=props.kind||'number';
    const axisValue=value=>kind==='percent'?(value*100).toFixed(0)+'%':formatValue(value,kind);
    useEffect(()=>{
      const L=root.LightweightCharts;
      if(failed||!L||!host.current)return;
      let chart;
      try{
        chart=L.createChart(host.current,{
          autoSize:true,
          layout:{background:{type:'solid',color:'#fafbfc'},textColor:'#64748b',fontSize:11,fontFamily:'ui-monospace, SFMono-Regular, Menlo, monospace',attributionLogo:true},
          grid:{vertLines:{color:'#e5e9ef'},horzLines:{color:'#e5e9ef'}},
          rightPriceScale:{borderColor:'#dce2ea',scaleMargins:{top:kind==='percent'?0:.12,bottom:.1},minimumWidth:52},
          leftPriceScale:{visible:false},
          timeScale:{borderColor:'#dce2ea',timeVisible:false,rightOffset:4,fixLeftEdge:true,fixRightEdge:true,minBarSpacing:.01},
          crosshair:{mode:L.CrosshairMode.Normal,vertLine:{color:'#64748b',width:1,style:2,labelBackgroundColor:'#334155'},horzLine:{visible:false,labelVisible:false}},
          handleScroll:{mouseWheel:false,pressedMouseMove:true,horzTouchDrag:true,vertTouchDrag:false},
          handleScale:{mouseWheel:!!props.expanded,pinch:true,axisPressedMouseMove:true,axisDoubleClickReset:true},
          localization:{locale:'ko-KR',priceFormatter:axisValue}
        });
        const state={chart,items:new Map(),references:[],firstFit:false};
        engine.current=state;
        const move=param=>{
          if(syncing.current)return;
          // Keep committed selections stable; hover remains local to its own chart.
          // A fresh click releases the old selection and commits a new date.
          if(pinned.current){
            const chosen=pinned.current,def=latest.current.series?.find(s=>s.data.some(r=>r.time===chosen&&Number.isFinite(r.value)));
            const point=def?.data.find(r=>r.time===chosen),item=def&&state.items.get(def.id);
            if(item&&point){syncing.current=true;try{chart.setCrosshairPosition(point.value,chosen,item.api);}catch{}finally{syncing.current=false;}}
            return;
          }
          const date=isoDay(param.time);
          if(!date){if(!pinned.current){setHover(null);setInspecting(false);}return;}
          setInspecting(true);
          setHover(prev=>prev===date?prev:date);
        };
        const click=param=>{const date=isoDay(param.time);if(!date)return;pinned.current=false;move(param);pinned.current=date;latest.current.onSelectDate?.(date);};
        chart.subscribeCrosshairMove(move);chart.subscribeClick(click);
        const rangeChanged=range=>{
          const from=isoDay(range?.from),to=isoDay(range?.to);
          if(from&&to)setVisibleRange(prev=>prev?.from===from&&prev?.to===to?prev:{from,to});
        };
        chart.timeScale().subscribeVisibleTimeRangeChange(rangeChanged);
        const observer=new ResizeObserver(entries=>{
          const rect=entries[0]?.contentRect;
          if(rect?.width>100&&rect.height>100&&!state.firstFit&&state.items.size){chart.timeScale().fitContent();state.firstFit=true;}
        });
        observer.observe(host.current);
        return ()=>{observer.disconnect();chart.timeScale().unsubscribeVisibleTimeRangeChange(rangeChanged);chart.unsubscribeCrosshairMove(move);chart.unsubscribeClick(click);engine.current=null;chart.remove();};
      }catch(error){if(chart)chart.remove();engine.current=null;setFailed(true);}
    },[kind,failed]);
    useEffect(()=>{
      const state=engine.current,L=root.LightweightCharts;if(!state)return;
      const keep=new Set(series.map(s=>s.id));
      for(const [id,item] of state.items){if(!keep.has(id)){item.markers?.detach();state.chart.removeSeries(item.api);state.items.delete(id);}}
      try{
        for(const def of series){
          let item=state.items.get(def.id);
          if(!item){item={api:state.chart.addSeries(L.LineSeries,{color:def.color,lineWidth:def.width||1,lastValueVisible:false,priceLineVisible:false,priceLineStyle:2,priceLineWidth:1,crosshairMarkerRadius:3,lineStyle:def.dashed?2:0,priceFormat:{type:'custom',minMove:kind==='percent' ? .0001 : .01,formatter:axisValue}}),markers:null};state.items.set(def.id,item);}
          item.api.setData(def.data);
        }
        state.chart.timeScale().fitContent();state.firstFit=!!host.current?.getBoundingClientRect().width;
        setHover(null);
      }catch(error){setFailed(true);}
    },[series,kind]);
    useEffect(()=>{
      const state=engine.current,L=root.LightweightCharts;if(!state)return;
      for(const def of series){const item=state.items.get(def.id);if(!item)continue;const points=showMarkers?markersForSeries(def,markers):[];if(item.markers)item.markers.setMarkers(points);else if(points.length)item.markers=L.createSeriesMarkers(item.api,points);}
    },[series,markers,kind,showMarkers]);
    useEffect(()=>{
      const state=engine.current;if(!state)return;
      for(const def of series)state.items.get(def.id)?.api.applyOptions({visible:!hidden[def.id]});
      state.chart.priceScale('right').applyOptions({mode:log&&kind!=='percent'?root.LightweightCharts.PriceScaleMode.Logarithmic:root.LightweightCharts.PriceScaleMode.Normal,autoScale:true});
    },[series,hidden,log,kind]);
    useEffect(()=>{
      const state=engine.current;if(!state)return;
      for(const item of state.items.values())for(const line of item.referenceLines||[])item.api.removePriceLine(line);
      for(const item of state.items.values())item.referenceLines=[];
      const first=series.find(s=>!hidden[s.id]),item=first&&state.items.get(first.id);
      if(kind==='percent')for(const target of state.items.values())target.api.applyOptions({autoscaleInfoProvider:original=>includeReferenceRange(original(),references)});
      if(item) for(const line of references)if(Number.isFinite(line.value))item.referenceLines.push(item.api.createPriceLine({price:line.value,color:line.color||'#94a3b8',lineWidth:1,lineStyle:2,axisLabelVisible:false,title:''}));
    },[references,series,hidden,kind]);
    useEffect(()=>{
      const state=engine.current,date=props.selectedDate;
      const explicitSelection=previousSelectionKey.current!==props.selectionKey;
      if(previousDate.current===date&&!explicitSelection)return;previousDate.current=date;previousSelectionKey.current=props.selectionKey;
      pinned.current=date;
      if(!state||!date)return;
      setInspecting(true);setHover(date);
      const def=series.find(s=>!hidden[s.id]&&s.data.some(r=>r.time===date&&Number.isFinite(r.value)));
      if(!def)return;
      const point=def.data.find(r=>r.time===date),item=state.items.get(def.id);if(!item)return;
      let frame,attempts=0,cancelled=false,fitted=false;
      function apply(){
        if(cancelled||engine.current!==state)return;
        const range=state.chart.timeScale().getVisibleRange();
        if(!fitted&&range&&(date<isoDay(range.from)||date>isoDay(range.to))){
          fitted=true;state.chart.timeScale().fitContent();frame=requestAnimationFrame(apply);return;
        }
        // A modal is initially display:none; wait for real price/time scales.
        const x=state.chart.timeScale().timeToCoordinate(date),y=item.api.priceToCoordinate(point.value);
        if(x===null||y===null){if(++attempts<12)frame=requestAnimationFrame(apply);return;}
        syncing.current=true;
        try{state.chart.setCrosshairPosition(point.value,date,item.api);}catch{}finally{syncing.current=false;}
      }
      frame=requestAnimationFrame(apply);
      return ()=>{cancelled=true;cancelAnimationFrame(frame);};
    },[props.selectedDate,props.selectionKey,series,hidden,kind]);
    if(!root.LightweightCharts||failed)return h('div',null,h('p',{className:'mb-2 text-xs text-app-subtext'},'기본 차트로 표시 중입니다.'),props.fallback);
    const date=hover||props.selectedDate;
    function zoom(factor){const ts=engine.current?.chart.timeScale(),r=ts?.getVisibleLogicalRange();if(!r)return;const mid=(r.from+r.to)/2,half=(r.to-r.from)*factor/2;ts.setVisibleLogicalRange({from:mid-half,to:mid+half});}
    function clear(){pinned.current=false;setInspecting(false);setHover(null);engine.current?.chart.clearCrosshairPosition();}
    const button=(label,action,active=false,extra={})=>h('button',{type:'button',onClick:action,className:`terminal-button${active?' active':''}`,...extra},label);
    return h('div',{className:'terminal-shell','data-testid':props.testId||'terminal-plot'},
      h('div',{className:'terminal-toolbar'},
        h('div',{className:'terminal-legend'},series.map(def=>h('span',{key:def.id,className:'terminal-series-label',style:{color:def.color,opacity:hidden[def.id] ? .35 : 1}},h('span',{className:'terminal-dot',style:{background:def.color}}),def.label))),
        h('details',{className:'terminal-settings'},h('summary',null,'차트 설정'),
          h('div',{className:'terminal-settings-body'},
            h('div',{className:'terminal-controls'},kind!=='percent'&&h(React.Fragment,null,button('선형',()=>setLog(false),!log,{'aria-pressed':!log}),button('로그',()=>setLog(true),log,{'aria-pressed':log})),button('−',()=>zoom(1.4),false,{'aria-label':'차트 축소',disabled:!visibleRange}),button('+',()=>zoom(.7),false,{'aria-label':'차트 확대',disabled:!visibleRange}),button('전체 구간',()=>engine.current?.chart.timeScale().fitContent())),
            h('div',{className:'terminal-controls'},series.filter(s=>s.toggleable).map(def=>button(def.label,()=>setHidden(prev=>({...prev,[def.id]:!prev[def.id]})),!hidden[def.id],{key:def.id,'aria-label':def.label+' 표시','aria-pressed':!hidden[def.id]})),markers.length>0&&button('교차·시점 표시',()=>setShowMarkers(v=>!v),showMarkers,{'aria-pressed':showMarkers})),
            h('p',{'data-testid':'terminal-visible-range'},visibleRange?`${visibleRange.from} ~ ${visibleRange.to}`:'전체 구간')
          )
        )
      ),
      h('div',{ref:host,className:'terminal-canvas',style:{height:props.height||'clamp(260px,34vw,340px)'},role:'group',tabIndex:0,onKeyDown:e=>{if(e.key==='Escape')clear();},'aria-label':props.label||'금융 차트'}),
      h('div',{className:'terminal-inspector'+(props.renderInspection?' with-prices':''),'data-testid':'terminal-inspector'},inspecting&&date?h(React.Fragment,null,
        h('div',{className:'terminal-inspector-date'},h('span',null,date),button('닫기',clear,false,{'aria-label':'선택값 닫기'})),
        props.renderInspection?props.renderInspection(date):h('div',{className:'terminal-inspector-values'},series.filter(def=>!hidden[def.id]).map(def=>h('span',{key:def.id,style:{color:def.color}},def.label+' ',h('strong',null,formatValue(def.data.find(r=>r.time===date)?.value,kind)))))
      ):h('p',{className:'terminal-idle-hint'},'터치·클릭으로 값 확인 · 가로 드래그로 이동'))
    );
  }

  const api={prepareSeries,markersForSeries,isoDay,formatValue,includeReferenceRange,Plot};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
  else root.ChartTerminal=api;
})(typeof window==='undefined'?globalThis:window);
