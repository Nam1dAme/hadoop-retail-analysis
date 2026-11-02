#!/usr/bin/env python3
"""Real pointer/keyboard, sidebar geometry and responsive checks on the running workbench."""
import json
from pathlib import Path
import sys
from playwright.sync_api import sync_playwright
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,atomic_json,sha,stamp,commit

OUT=ROOT/'var/production/ui-review';OUT.mkdir(exist_ok=True)
PAGES={'overview':'行为总览','entities':'商品与类目','activity':'用户活跃','predictions':'行为预测','experiments':'模型实验','quality':'数据质量与任务','versions':'版本与方法'}
report=dict(status='RUNNING',started_at=stamp(),code_commit=commit(),theme_sha256=sha(ROOT/'app/assets/theme.css'),config_sha256=sha(ROOT/'.streamlit/config.toml'),pages=[],sidebar=[],buttons=[],screenshots=[])
atomic_json(OUT/'verification.json',report)

def ready(page,name):
    page.locator(f'[data-behavior-page-ready="{name}"],[data-testid="stAlertContentError"],[data-testid="stException"]').first.wait_for(state='attached',timeout=30000)
    assert not page.locator('[data-testid="stAlertContentError"],[data-testid="stException"]').count(),page.locator('body').inner_text()[-600:]
    page.evaluate('document.fonts.ready')

def expanded(page,value):
    sidebar=page.locator('[data-testid="stSidebar"]')
    if (sidebar.get_attribute('aria-expanded')=='true')!=value:
        control=page.locator('[data-testid="stSidebarCollapseButton"] button' if not value else '[data-testid="stSidebarCollapsedControl"] button')
        control.click();page.wait_for_timeout(350)
    assert (sidebar.get_attribute('aria-expanded')=='true')==value

def layout(page,sidebar=False):
    return page.evaluate('''(sidebar)=>{
      const root=sidebar?document.querySelector('[data-testid="stSidebarContent"]'):document.querySelector('[data-testid="stMainBlockContainer"]');
      const bounds=root.getBoundingClientRect(),bad=[];
      const walker=document.createTreeWalker(root,NodeFilter.SHOW_TEXT);
      while(walker.nextNode()){
        const node=walker.currentNode,e=node.parentElement;if(!node.textContent.trim()||!e)continue;
        if(e.closest('details:not([open]),style,script,svg,pre,[data-testid="stDataFrame"],[data-testid="stJson"],[data-testid="stCode"]'))continue;
        const s=getComputedStyle(e);if(s.display==='none'||s.visibility==='hidden'||s.opacity==='0')continue;
        let clipped=false;for(let a=e;a&&a!==root;a=a.parentElement){const c=getComputedStyle(a);if(c.clip!=='auto'||c.clipPath!=='none'||c.textOverflow==='ellipsis'||(c.overflow==='hidden'&&a.clientWidth<=2)){clipped=true;break;}}if(clipped)continue;
        const card=e.closest('[data-testid="stVerticalBlock"][class*="st-key-panel_"]');
        const local=card?card.getBoundingClientRect():bounds;
        const range=document.createRange();range.selectNodeContents(node);
        for(const r of range.getClientRects()){
          if(!r.width||!r.height)continue;
          if(r.left<Math.max(0,local.left)-2||r.right>Math.min(innerWidth,local.right)+2)bad.push({text:node.textContent.trim().slice(0,100),left:r.left,right:r.right,boundLeft:local.left,boundRight:local.right});
        }
      }
      if(!sidebar)for(const e of root.querySelectorAll('[data-testid="stPlotlyChart"],[data-testid="stDataFrame"],[data-testid="stSelectbox"]')){
        const card=e.closest('[data-testid="stVerticalBlock"][class*="st-key-panel_"]');if(!card||e.closest('details:not([open])'))continue;
        const r=e.getBoundingClientRect(),c=card.getBoundingClientRect();if(!r.width||!r.height)continue;if(r.left<c.left-2||r.right>c.right+2)bad.push({widget:e.dataset.testid,left:r.left,right:r.right,boundRight:c.right});
        for(const svg of e.querySelectorAll('svg.main-svg'))if(svg.getBoundingClientRect().width>r.width+2)bad.push({widget:'plotly svg',width:svg.getBoundingClientRect().width,containerWidth:r.width});
      }
      const scroller=sidebar?root:document.querySelector('[data-testid="stMain"]');
      return {viewport:innerWidth,documentWidth:document.documentElement.scrollWidth,containerWidth:scroller.clientWidth,scrollWidth:scroller.scrollWidth,bad};
    }''',sidebar)

def colors(button):
    return button.evaluate('(e)=>({text:getComputedStyle(e).color,border:getComputedStyle(e).borderColor,background:getComputedStyle(e).backgroundColor})')

def no_red(state):
    import re
    for value in state.values():
        rgb=re.search(r'rgba?\((\d+), (\d+), (\d+)',value)
        if rgb:
            r,g,b=map(int,rgb.groups());assert not (r>180 and r>g*1.35 and r>b*1.35),state

try:
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True,args=['--disable-gpu'])
        for width,height in ((1440,1000),(1280,720),(768,900),(390,844),(320,640)):
            context=browser.new_context(viewport={'width':width,'height':height});page=context.new_page()
            page.goto('http://127.0.0.1:8510/?page=overview',wait_until='domcontentloaded');ready(page,'overview')
            expanded(page,True);page.mouse.move(width-4,400);page.wait_for_timeout(200)
            labels=page.locator('.st-key-navigation label[data-baseweb="radio"]')
            before=labels.evaluate_all('(nodes)=>nodes.map(n=>{let r=n.getBoundingClientRect();return [r.x,r.y,r.width,r.height]})')
            displacement=0
            for i in range(7):
                labels.nth(i).hover();page.wait_for_timeout(80)
                after=labels.evaluate_all('(nodes)=>nodes.map(n=>{let r=n.getBoundingClientRect();return [r.x,r.y,r.width,r.height]})')
                displacement=max(displacement,max(abs(a-b) for row,other in zip(before,after) for a,b in zip(row,other)))
                assert displacement<.6,('sidebar moved',width,i,before,after)
            geometry=layout(page,True);assert not geometry['bad'],('sidebar text overflow',width,geometry)
            assert geometry['scrollWidth']<=geometry['containerWidth']+2,('sidebar overflow',width,geometry)
            assert page.locator('[data-testid="stSidebar"]').bounding_box()['width']<=width
            path=OUT/f'sidebar-top-{width}.png';page.screenshot(path=str(path));report['screenshots'].append(str(path))
            # Header remains available even in short viewports after scrolling down.
            page.locator('[data-testid="stSidebarContent"]').evaluate('(e)=>e.scrollTop=e.scrollHeight')
            page.wait_for_timeout(100)
            close=page.locator('[data-testid="stSidebarCollapseButton"] button').bounding_box();assert close and close['y']>=0 and close['y']+close['height']<=height
            path=OUT/f'sidebar-{width}.png';page.screenshot(path=str(path));report['screenshots'].append(str(path))
            report['sidebar'].append(dict(width=width,hover_displacement=displacement,geometry=geometry))
            page.locator('[data-testid="stSidebarContent"]').evaluate('(e)=>e.scrollTop=0')
            expanded(page,False)
            opener=page.locator('[data-testid="stSidebarCollapsedControl"] button').bounding_box()
            crumb=page.locator('.page-crumb').evaluate('(e)=>{let r=document.createRange();r.selectNodeContents(e);return r.getBoundingClientRect().toJSON()}')
            assert not (crumb['left']<opener['x']+opener['width'] and crumb['right']>opener['x'] and crumb['top']<opener['y']+opener['height'] and crumb['bottom']>opener['y']),('collapsed header overlap',width,crumb,opener)
            if width>=1280:expanded(page,True)
            if width==1440:
                for target in ('entities','experiments','overview'):
                    page.locator('.st-key-navigation label[data-baseweb="radio"]').filter(has_text=PAGES[target]).click();ready(page,target)
                    assert 'page='+target in page.url
            for name,title in PAGES.items():
                page.goto('http://127.0.0.1:8510/?page='+name,wait_until='domcontentloaded');ready(page,name)
                if width<1280:expanded(page,False)
                for detail in page.locator('details').all():
                    if detail.get_attribute('open') is None:detail.locator('summary').click()
                geometry=layout(page);assert geometry['documentWidth']<=width+2 and geometry['scrollWidth']<=geometry['containerWidth']+2,('main horizontal overflow',width,name,geometry)
                assert not geometry['bad'],('text overflow',width,name,geometry)
                actions=page.locator('[data-testid="stButton"] button,[data-testid="stFormSubmitButton"] button,[data-testid="stDownloadButton"] button,[data-testid="stLinkButton"] a')
                for i in range(actions.count()):
                    button=actions.nth(i)
                    if not button.is_visible() or not button.is_enabled():continue
                    button.hover();page.wait_for_timeout(50);state=colors(button);no_red(state)
                    box=button.bounding_box();page.mouse.down();pressed=colors(button);page.mouse.move(width-2,2);page.mouse.up();no_red(pressed)
                    button.focus();page.keyboard.press('Tab');page.keyboard.press('Shift+Tab')
                    focus=button.evaluate('(e)=>({outline:getComputedStyle(e).outlineStyle,color:getComputedStyle(e).outlineColor,width:getComputedStyle(e).outlineWidth})')
                    assert focus['outline']!='none' and focus['width']!='0px',('missing keyboard focus',name,focus)
                    report['buttons'].append(dict(width=width,page=name,label=button.inner_text(),hover=state,pressed=pressed,focus=focus))
                if name=='overview':
                    page.locator('[data-testid="stDateInput"] input').first.click()
                    calendar=page.locator('[data-baseweb="calendar"]');calendar.wait_for(state='visible')
                    box=calendar.bounding_box();assert box['x']>=-2 and box['x']+box['width']<=width+2,('calendar overflow',width,box)
                    page.keyboard.press('Escape')
                if name=='experiments':
                    page.get_by_role('combobox').first.click()
                    popup=page.locator('[data-testid="stSelectboxVirtualDropdown"]');popup.wait_for(state='visible')
                    box=popup.bounding_box();assert box['x']>=-2 and box['x']+box['width']<=width+2,('select overflow',width,box)
                    page.keyboard.press('Escape')
                page.locator('[data-testid="stMain"]').evaluate('(e)=>e.scrollTop=0')
                page.get_by_role('heading',name=title,exact=True).click();page.mouse.move(width-3,2)
                path=OUT/f'{name}-{width}.png';page.screenshot(path=str(path));report['screenshots'].append(str(path))
                report['pages'].append(dict(width=width,page=name,geometry=geometry))
            context.close()
        browser.close()
    report.update(status='PASS',verified_at=stamp());atomic_json(OUT/'verification.json',report)
    print(json.dumps(dict(status='PASS',pages=len(report['pages']),sidebar_widths=len(report['sidebar']),button_states=len(report['buttons']))))
except BaseException as error:
    report.update(status='FAIL',error_type=type(error).__name__,error=str(error),verified_at=stamp());atomic_json(OUT/'verification.json',report);raise
