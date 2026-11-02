#!/usr/bin/env python3
"""Rebuild local UI glyph subsets from licensed upstream TTFs; no font download."""
import argparse
import hashlib
import json
from pathlib import Path
from fontTools.ttLib import TTFont
from fontTools import subset

ROOT=Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser();parser.add_argument('--source-dir',type=Path,required=True);args=parser.parse_args()
text=''.join(p.read_text() for folder in ('app','docs') for p in (ROOT/folder).rglob('*') if p.suffix in ('.py','.md','.css'))
codepoints={ord(c) for c in text}|set(range(0x20,0x250))|{0x2009,0x202F,0x2212}
records=[]
for upstream,target,family in [('IBMPlexSans','plex','Retail Numerals'),('NotoSansSC','noto','Retail Chinese')]:
    source=args.source_dir/(upstream+'.ttf');font=TTFont(source)
    options=subset.Options();options.name_IDs=['*'];options.name_legacy=True;options.name_languages=['*'];options.layout_features=['*']
    worker=subset.Subsetter(options=options);worker.populate(unicodes=codepoints);worker.subset(font)
    for record in font['name'].names:
        if record.nameID in (1,4,6,16,21):record.string=(family.replace(' ','') if record.nameID==6 else family).encode(record.getEncoding(),errors='replace')
        elif record.nameID in (2,17,22):record.string='Regular'.encode(record.getEncoding(),errors='replace')
    font.flavor='woff2';destination=ROOT/'app/assets'/(target+'.woff2');font.save(destination)
    records.append(dict(upstream=upstream,family=family,source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                        subset_sha256=hashlib.sha256(destination.read_bytes()).hexdigest(),bytes=destination.stat().st_size,
                        glyph_source='Current behavior UI and project-document characters plus U+0020..U+024F',weight_range=[400,700]))
(ROOT/'app/assets/fonts.json').write_text(json.dumps(records,indent=2)+'\n')
available=set()
for p in (ROOT/'app/assets').glob('*.woff2'):
    with TTFont(p) as font:available.update(font.getBestCmap())
required={ord(c) for p in (ROOT/'app').glob('*.py') for c in p.read_text() if '\u4e00'<=c<='\u9fff'}
assert required<=available, 'Missing visible Chinese glyphs'
print(json.dumps(dict(status='PASS',ui_chinese_glyphs=len(required),files=records)))
