const express = require('express');
const router = express.Router();
const { PythonShell } = require('python-shell');
const path = require('path');
const fs = require('fs');

// uploads 폴더의 최신 파일 찾기 (filename 미전달 시 대비)
function getLatestUpload() {
  const dir = path.join(__dirname, '..', 'uploads');
  const files = fs.readdirSync(dir).filter(f => !f.startsWith('.'));
  if (!files.length) throw new Error('업로드된 파일이 없습니다.');
  const full = files
    .map(f => ({ f, t: fs.statSync(path.join(dir, f)).mtimeMs }))
    .sort((a,b) => b.t - a.t)[0].f;
  return path.join(dir, full);
}

function runPy(scriptName, args) {
  return new Promise((resolve, reject) => {
    const options = {
      mode: 'json',
      pythonOptions: ['-u'],
      scriptPath: path.join(__dirname, '..', 'scripts'),
      args,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
    };
    PythonShell.run(`${scriptName}.py`, options, (err, results) => {
      if (err) return reject(err);
      if (!results || !results.length) return reject(new Error('No result from Python'));
      resolve(results[0]);
    });
  });
}

// 기초 통계량: /stat/basic  { column, filename? }
router.post('/basic', async (req, res) => {
  try {
    const { column, filename } = req.body || {};
    if (!column) return res.status(400).json({ error: 'column이 필요합니다.' });

    const filePath = filename
      ? path.join(__dirname, '..', 'uploads', filename)
      : getLatestUpload();

    const data = await runPy('basic_stat', [filePath, column]);
    res.json(data);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// 분포 시각화(히스토그램): /stat/distribution  { column, filename? }
router.post('/distribution', async (req, res) => {
  try {
    const { column, filename } = req.body || {};
    if (!column) return res.status(400).json({ error: 'column이 필요합니다.' });

    const filePath = filename
      ? path.join(__dirname, '..', 'uploads', filename)
      : getLatestUpload();

    const data = await runPy('distribution', [filePath, column]);
    // 결과: { image: "파일명.png" } -> 정적 제공
    res.json(data);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// 상관분석: /stat/correlation  { column?, filename? }  // column은 프론트 호환용
router.post('/correlation', async (req, res) => {
  try {
    const { column, filename } = req.body || {};
    const filePath = filename
      ? path.join(__dirname, '..', 'uploads', filename)
      : getLatestUpload();

    const data = await runPy('correlation', [filePath, column || 'ALL']);
    res.json(data);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e) });
  }
});

// t-검정: /stat/ttest
// body: { valueCol, groupCol, groupA, groupB, filename?, equal_var? }
router.post('/ttest', async (req, res) => {
    try {
      const { valueCol, groupCol, groupA, groupB, filename, equal_var } = req.body || {};
      if (!valueCol || !groupCol || groupA === undefined || groupB === undefined) {
        return res.status(400).json({ error: 'valueCol, groupCol, groupA, groupB 필요' });
      }
      const filePath = filename
        ? path.join(__dirname, '..', 'uploads', filename)
        : getLatestUpload();
  
      const args = [filePath, valueCol, groupCol, String(groupA), String(groupB), String(!!equal_var)];
      const data = await runPy('ttest', args);
      res.json(data);
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: String(e) });
    }
  });
  
  // 카이제곱: /stat/chi2
  // body: { colA, colB, filename? }
  router.post('/chi2', async (req, res) => {
    try {
      const { colA, colB, filename } = req.body || {};
      if (!colA || !colB) return res.status(400).json({ error: 'colA, colB 필요' });
  
      const filePath = filename
        ? path.join(__dirname, '..', 'uploads', filename)
        : getLatestUpload();
  
      const data = await runPy('chi2', [filePath, colA, colB]);
      res.json(data);
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: String(e) });
    }
  });
  
  // 선형회귀: /stat/linreg
  // body: { target, features: string[], filename? }
  router.post('/linreg', async (req, res) => {
    try {
      const { target, features, filename } = req.body || {};
      if (!target || !Array.isArray(features) || features.length === 0) {
        return res.status(400).json({ error: 'target, features[] 필요' });
      }
  
      const filePath = filename
        ? path.join(__dirname, '..', 'uploads', filename)
        : getLatestUpload();
  
      const featCSV = features.join(',');
      const data = await runPy('linreg', [filePath, target, featCSV]);
      res.json(data);
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: String(e) });
    }
  });


// outputs 폴더 정적 제공 (히스토그램 보기용)
router.use('/images', express.static(path.join(__dirname, '..', 'outputs')));

module.exports = router;
