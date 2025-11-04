import express from 'express';
// 실제 구현 시 AWS SDK v3(S3) 또는 Cloudflare R2/네이버 OBS 사용
// 여기서는 인터페이스만 정의

const router = express.Router();

// 클라가 호출 → 서버가 버킷에 PUT용 signed URL 발급
router.post('/sign', async (req, res) => {
  const { name, mime } = req.body || {};
  // TODO: 실제 presign 코드 (S3: @aws-sdk/s3-request-presigner)
  // const url = await getPresignedUrl({ Key, ContentType: mime, Expires: 3600 });
  const url = `https://example.com/fake-signed-url/${encodeURIComponent(name)}`; // placeholder
  const key = `tmp/${Date.now()}-${name}`;
  res.json({ url, key, ttlSec: 3600 });
});

// 작업 종료 후 즉시 삭제
router.post('/delete', async (req, res) => {
  const { key } = req.body || {};
  // TODO: 버킷에서 key 삭제
  res.json({ ok:true, key });
});

export default router;
