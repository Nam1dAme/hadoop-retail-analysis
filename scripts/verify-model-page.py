#!/usr/bin/env python3
"""Supplement seven-page checks with actual selected-model plots and resources."""
import json
from pathlib import Path
import sys
from playwright.sync_api import sync_playwright
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,atomic_json,sha,stamp,commit

output=ROOT/'var/production/browser-selected-model';output.mkdir(exist_ok=True)
release=json.loads((ROOT/'var/production/release.json').read_text())['release_id']
models=json.loads((ROOT/'var/production/models.json').read_text());selected=next(m for m in models['selection']['finalists'] if m['model_run_id']==models['selected_model_id'])['algorithm']
report=dict(status='RUNNING',scope='Selected-model page and real training/resources panels; supplements the seven-page browser report',
            release_id=release,selected_algorithm=selected,code_commit=commit(),environment_sha256=sha(ROOT/'config/environment.lock.json'),
            command='scripts/verify-model-page.py with Playwright',screenshots=[],started_at=stamp())
atomic_json(output/'verification.json',report)
try:
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True,args=['--disable-gpu'])
        for width,height in ((1600,1100),(390,844)):
            context=browser.new_context(viewport={'width':width,'height':height});page=context.new_page()
            page.goto('http://127.0.0.1:8510/?page=experiments',wait_until='domcontentloaded')
            page.locator('[data-behavior-page-ready="experiments"],[data-testid="stAlertContentError"],[data-testid="stException"]').first.wait_for(state='attached',timeout=30000)
            assert not page.locator('[data-testid="stException"],[data-testid="stAlertContentError"]').count()
            assert release in page.locator('.context-bar').inner_text()
            assert selected in page.locator('[data-testid="stSelectbox"]').first.inner_text()
            assert page.evaluate('document.documentElement.scrollWidth')<=width+2
            page.wait_for_timeout(1000)
            if selected!='CONSTANT':
                training=page.locator('.st-key-panel_training').text_content()
                assert '没有训练曲线' not in training and '验证 AP' in training
            for label,selector in [('page',None),('training','.st-key-panel_training'),('resources','.st-key-panel_model_resources')]:
                path=output/f'{label}-{width}.png'
                if selector:page.locator(selector).screenshot(path=str(path))
                else:page.screenshot(path=str(path))
                report['screenshots'].append(dict(kind=label,width=width,path=str(path),sha256=sha(path)))
            context.close()
        browser.close()
    report.update(status='PASS',verified_at=stamp());atomic_json(output/'verification.json',report)
    print(json.dumps(dict(status='PASS',screenshots=len(report['screenshots']),selected_algorithm=selected)))
except BaseException as error:
    report.update(status='FAIL',error_type=type(error).__name__,message=str(error),verified_at=stamp());atomic_json(output/'verification.json',report);raise
