"""Read-only source/index checks and local README rendering. Never starts training."""
import functools,http.server,json,os,re,subprocess,threading
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def main():
    from markdown_it import MarkdownIt
    from playwright.sync_api import sync_playwright
    out=ROOT/'artifacts/publication-review';out.mkdir(parents=True,exist_ok=True)
    candidates=sorted(set(subprocess.check_output(['git','ls-files','-z','--cached','--others','--exclude-standard'],cwd=ROOT).decode().split('\0'))-{''})
    blocked=[];large=[];sensitive=[]
    patterns=[re.compile(r'\bAKIA[A-Z0-9]{16}\b'),re.compile(r'\bghp_[A-Za-z0-9]{36}\b'),re.compile(r'(?m)^-----BEGIN (?:RSA |OPENSSH )?PRIVATE KEY-----'),re.compile(r'C:[/\\]Users[/\\]Emmu',re.I)]
    for name in candidates:
        p=ROOT/name
        if name.startswith(('data/','models/','artifacts/','logs/','.venv/','notebooks/legacy_taxi/')) or name=='.env' or name.endswith('.generated'):blocked.append(name)
        if not p.is_file():continue
        if p.stat().st_size>2*1024**2:large.append(name)
        if p.suffix in {'.py','.md','.yml','.yaml','.json','.ps1','.sh','.txt','.sql'}:
            text=p.read_text(encoding='utf-8-sig',errors='replace')
            if any(pattern.search(text) for pattern in patterns):sensitive.append(name)
    # This checker contains a sample personal-path regex, not an actual path.
    assert not blocked and not large and not sensitive,(blocked,large,sensitive)
    (out/'staging-candidates.txt').write_text('\n'.join(candidates)+'\n')
    readme=(ROOT/'README.md').read_text();html=MarkdownIt('commonmark',{'html':False}).enable('table').render(readme)
    preview=out/'readme-preview.html'
    preview.write_text('<!doctype html><html><head><meta charset="utf-8"><base href="/"><title>Citi Bike README review</title><style>body{max-width:1100px;margin:40px auto;padding:24px;font:16px/1.65 system-ui;color:#172033}img{max-width:100%}table{border-collapse:collapse;width:100%}td,th{padding:9px;border:1px solid #dce2eb}pre{background:#f3f6fa;padding:16px;overflow:auto}h1,h2,h3{line-height:1.3}a{color:#2563eb}</style></head><body>'+html+'</body></html>')
    class Handler(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*args):pass
    server=http.server.ThreadingHTTPServer(('127.0.0.1',0),functools.partial(Handler,directory=str(ROOT)))
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    os.environ.setdefault('PLAYWRIGHT_BROWSERS_PATH',str(ROOT/'artifacts/browser-cache'))
    report={'candidate_files':len(candidates),'blocked_paths':blocked,'large_files_over_2_mib':large,'credential_pattern_or_personal_path_findings':sensitive,'training_started':False,'deployment':False}
    try:
        with sync_playwright() as pw:
            browser=pw.chromium.launch(headless=True);page=browser.new_page(viewport={'width':1400,'height':1000})
            errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
            base=f'http://127.0.0.1:{server.server_port}'
            page.goto(base+'/artifacts/publication-review/readme-preview.html',wait_until='networkidle')
            images=page.locator('img').evaluate_all('(images)=>images.map(i=>({alt:i.alt,loaded:i.complete&&i.naturalWidth>0}))')
            assert len(images)==6 and all(i['loaded'] and i['alt'] for i in images),images
            image_links=sorted(set(re.findall(r'!?\[[^\]]*\]\((docs/images/[^)]+)\)',readme)))
            assert len(image_links)==7
            for link in image_links:
                response=page.request.get(base+'/'+link)
                assert response.status==200,link
                if link.endswith('.png'):
                    from PIL import Image
                    import io
                    with Image.open(io.BytesIO(response.body())) as image:image.verify()
                else:
                    import xml.etree.ElementTree as ET
                    assert ET.fromstring(response.body()).tag.endswith('svg')
            report['all_seven_readme_visuals_verified']=True
            page.screenshot(path=str(out/'readme-render.png'),full_page=True)
            page.goto(base+'/docs/images/citibike-architecture.svg',wait_until='networkidle')
            assert page.locator('svg').count()==1 and page.locator('text').count()>15
            # Capture the SVG as an HTML image: Chromium full-page capture of a
            # standalone XML/SVG document is not reliable across versions.
            page.goto('about:blank')
            page.set_content(f'<html><body><img src="{base}/docs/images/citibike-architecture.svg" style="width:1300px"></body></html>',wait_until='networkidle')
            page.wait_for_function('document.images[0].complete && document.images[0].naturalWidth > 0')
            page.locator('img').screenshot(path=str(out/'architecture-render.png'))
            assert not errors,errors
            report.update(readme_images=images,svg_browser_rendered=True,browser_errors=errors)
            browser.close()
    finally:server.shutdown();server.server_close();thread.join(5)
    import xml.etree.ElementTree as ET
    suite=ET.parse(out/'tests.xml').getroot().find('testsuite')
    report['pytest']={k:int(suite.attrib[k]) for k in ['tests','failures','errors','skipped']}
    report['pytest']['passed']=report['pytest']['tests']-report['pytest']['failures']-report['pytest']['errors']-report['pytest']['skipped']
    report['scan_scope']='Current Git candidates; selected credential formats and personal paths only. Not a complete secret/history audit.'
    (out/'checks.json').write_text(json.dumps(report,indent=2)+'\n')
    (ROOT/'docs/audit/publication_checks.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__=='__main__':main()
