import './globals.css';

export const metadata = {
  title: 'CQS AI',
  description: 'AI 全球通用智能发票与报表生成器',
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="zh-CN">
      <body>{children}</body>
    </html>
  );
}