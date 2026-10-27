/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // This service only renders the DAG design/observability surface; it never
  // executes load itself, so no server-side runtime beyond Next.js is needed.
  env: {
    NEXT_PUBLIC_PIPELINE_API_GATEWAY_URL:
      process.env.NEXT_PUBLIC_PIPELINE_API_GATEWAY_URL ?? 'http://localhost:4000/graphql',
  },
};

module.exports = nextConfig;
