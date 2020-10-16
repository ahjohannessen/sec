module.exports = {
  title: 'Scala EventStoreDB Client',
  tagline: 'A pure functional client for EventStoreDB',
  url: 'https://github.com/ahjohannessen/sec',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  favicon: 'img/favicon.ico',
  organizationName: 'ahjohannessen',
  projectName: 'sec',
  themeConfig: {
    sidebarCollapsible: false,
    colorMode: {
      defaultMode: 'light',
      respectPrefersColorScheme: true
    },
    navbar: {
      title: ' ',
      logo: {
        alt: 'sec',
        src: 'img/logo.svg',
      },
      items: [
        {
          to: 'docs/',
          activeBasePath: 'docs',
          label: 'Docs',
          position: 'left',
        },
        {
          href: 'https://github.com/ahjohannessen/sec',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      copyright: `Copyright © ${new Date().getFullYear()}, sec contributors.`,
    },
    prism: {
      additionalLanguages: ['scala', 'protobuf']
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          path: '../sec-docs/target/mdoc/'
        },
        blog: { },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
};
