# S3 Downloader

Spring Boot based app for browsing/downloading S3-compatible sources.

## Essentials

- Requirements: Java 21+, network access to your S3-compatible service.
- Configure via env vars (repeat the block for each source):

```
export STORAGE_S3_DEFAULT_SOURCE=primary
export STORAGE_S3_PRIMARY_REGION=your-region
export STORAGE_S3_PRIMARY_ENDPOINT=https://your-endpoint
export STORAGE_S3_PRIMARY_ACCESS_KEY=your-access
export STORAGE_S3_PRIMARY_SECRET_KEY=your-secret
export STORAGE_S3_PRIMARY_BUCKET=your-bucket
```

- Run app: `./gradlew bootRun`
- Run tests: `./gradlew test`
