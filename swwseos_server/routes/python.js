const express = require("express");
const multer = require("multer");
const { spawn } = require("child_process");
const path = require("path");
const { Console } = require("console");
PYTHON_BIN=/usr/bin/python3

const router = express.Router();  // ✅ 추가
// ✅ storage를 먼저 선언한 후 multer 사용!
const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, path.join(__dirname, "../uploads")); // ✅ 절대 경로 사용
    },
    filename: function (req, file, cb) {
        cb(null, Date.now() + "-" + file.originalname);
    },
});

const upload_data = multer({ storage: storage });

// ✅ 파일 업로드 API
router.post("/upload", upload_data.single("file"), (req, res) => {
    console.log("📂 파일 업로드 요청 도착");
    console.log("📂 업로드된 파일 정보:", req.file);  // ✅ 파일 정보 출력

    if (!req.file) {
        console.error("❌ 파일이 업로드되지 않았습니다.");
        return res.status(400).json({ error: "파일 업로드 실패" });
    }

    res.json({ message: "파일 업로드 완료", filename: req.file.filename });
});

// ✅ Python 스크립트 실행 (파일 경로 전달)
router.post("/process", (req, res) => {
    console.log("📢 /process API 호출됨!");
    console.log("📂 요청된 파일 이름:", req.body.filename);  // ✅ 파일 이름 확인

    if (!req.body.filename) {
        console.error("❌ 파일 이름이 전달되지 않았습니다.");
        return res.status(400).json({ error: "파일 처리 실패: 파일 이름이 전달되지 않았습니다." });
    }

    // ✅ 업로드된 파일 경로
    const filePath = path.join(__dirname, "../uploads", req.body.filename);

    console.log("🚀 Python 스크립트 실행 시작:", filePath);
    const pythonProcess = spawn("C:\\Users\\user\\anaconda3\\envs\\ngnl\\python.exe", [
        "scripts/load_file.py",
        filePath
    ], { env: { ...process.env, PYTHONUTF8: "1" } });

    let dataString = "";
    pythonProcess.stdout.on("data", (data) => {
        dataString += data.toString();
        console.log("📢 Python 응답:", dataString);
    });

    pythonProcess.stderr.on("data", (data) => {
        console.error("❌ Python 오류:", data.toString());
    });

    pythonProcess.on("close", (code) => {
        console.log(`⚡ Python 프로세스 종료 (코드: ${code})`);
        if (code === 0) {
            try {
                const jsonData = JSON.parse(dataString);
                res.json(jsonData);
            } catch (err) {
                console.error("❌ JSON 파싱 오류:", err);
                res.status(500).json({ error: "Python 처리 중 오류 발생" });
            }
        } else {
            res.status(500).json({ error: "Python 스크립트 실행 실패" });
        }
    });
});
console.log("🧪 /generate-graph 라우트 선언 직전 도달");
router.post("/generate-graph", (req, res) => {
    console.log("📢 /generate-graph API 호출됨!");

    const { xColumn, yColumn, data } = req.body;

    console.log("📂 요청 데이터:", { xColumn, yColumn });  // ✅ 데이터 확인 로그 추가

    if (!xColumn || !yColumn || !data) {
        return res.status(400).json({ error: "필요한 데이터가 부족합니다." });
    }

    const pythonProcess = spawn("C:\\Users\\user\\anaconda3\\envs\\ngnl\\python.exe", [
        "scripts/generate_graph.py",
        JSON.stringify(data),
        xColumn,
        yColumn,
    ]);

    let imageBuffer = [];

    pythonProcess.stdout.on("data", (data) => {
        imageBuffer.push(data);
    });

    pythonProcess.stderr.on("data", (data) => {  // ✅ 에러 로그 확인
        console.error(`❌ Python 오류: ${data}`);
    });

    pythonProcess.on("close", (code) => {
        console.log(`⚡ Python 프로세스 종료 (코드: ${code})`);
        if (code === 0) {
            
            const imageBase64 = Buffer.concat(imageBuffer).toString('base64');
            res.json({ image: imageBase64 });
        } else {
            res.status(500).json({ error: "그래프 생성 실패" });
        }
    });
});


module.exports = router;  // ✅ 추가
