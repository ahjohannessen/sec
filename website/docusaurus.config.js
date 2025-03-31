// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Scala KurrentDB Client',
  tagline: 'A pure functional client for KurrentDB',
  favicon: 'img/favicon.ico',
  url: 'https://ahjohannessen.github.com',
  baseUrl: '/sec/',
  organizationName: 'ahjohannessen',
  projectName: 'sec',

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          path: '../sec-docs/target/mdoc/'
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      colorMode: {
        defaultMode: 'dark',
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
            to: 'docs',
            label: 'Docs',
            activeBasePath: 'docs',
            position: 'left'
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
        copyright: `Copyright © ${new Date().getFullYear()}, sec contributors.`
      },
      prism: {
        additionalLanguages: ['java', 'scala', 'protobuf']
      },
    }),
};

module.exports = config;
