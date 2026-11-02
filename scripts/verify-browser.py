#!/usr/bin/env python3
"""Real-browser screenshots and responsive checks using an explicitly supplied browser cache."""
from pathlib import Path
import sys
import json
import time
from playwright.sync_api import sync_playwright
ROOT=Path(__file__).resolve().parents[1]
output=ROOT/'var/production/browser';output.mkdir(parents=True,exist_ok=True)
pages={'overview':'行为总览','entities':'商品与类目','activity':'用户活跃','predictions':'行为预测',
       'experiments':'模型实验','quality':'数据质量与任务','versions':'版本与方法'}
results=[]
with sync_playwright() as p:
    browser=p.chromium.launch(headless=True,args=['--disable-gpu'])
    for width,height in ((1600,1100),(390,844)):
        context=browser.new_context(viewport={'width':width,'height':height},device_scale_factor=1)
        page=context.new_page();errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
        for name,label in pages.items():
            page.goto('http://127.0.0.1:8510/?page='+name,wait_until='domcontentloaded')
            page.get_by_role('heading',name=label,exact=True).wait_for(timeout=30000)
            page.get_by_text('数据版本已固定',exact=False).wait_for(timeout=30000)
            # Wait for all initial API reads/charts and Streamlit's script runner to settle.
            page.wait_for_timeout(2000)
            if page.locator('[data-testid="stException"]').count():raise AssertionError('Streamlit exception: '+name)
            if page.locator('[data-testid="stAlert"]').filter(has_text='无法连接').count():raise AssertionError('API unavailable')
            geometry=page.evaluate('({width:innerWidth,scrollWidth:document.documentElement.scrollWidth})')
            if geometry['scrollWidth']>width+2:raise AssertionError('Horizontal page overflow: '+name+' '+str(geometry))
            path=output/f'{name}-{width}.png';page.screenshot(path=str(path),full_page=True)
            results.append(dict(page=name,width=width,path=str(path),geometry=geometry))
        if errors:raise AssertionError(errors)
        context.close()
    browser.close()
(output/'verification.json').write_text(json.dumps(dict(status='PASS',screenshots=results),ensure_ascii=False,indent=2)+'\n')
print(json.dumps(dict(status='PASS',screenshots=len(results),output=str(output))))
