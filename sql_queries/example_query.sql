-- 예시 쿼리: 사용자 목록 조회
SELECT 
    id,
    username,
    email,
    created_at
FROM users
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY created_at DESC;
