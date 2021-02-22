import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './styles.module.css';

const features = [
  {
    title: 'Principled and Functional',
    imageUrl: 'img/functional.svg',
    description: (
      <>
        Use the power of functional programming and Scala to build robust, correct
        and type-safe applications that interact with <a href="https://www.eventstore.com" target="_blank">EventStoreDB</a> nodes in a purely functional style.
      </>
    ),
  },
  {
    title: 'Safe and Asynchronous IO',
    imageUrl: 'img/async.svg',
    description: (
      <>
        Uses <a href="https://typelevel.org/cats-effect" target="_blank">Cats Effect</a> such you get fully asynchronous,
        non-blocking, composable and safe building blocks for your eventsourced applications.
      </>
    ),
  },
  {
    title: 'Powerful Streaming',
    imageUrl: 'img/streaming.svg',
    description: (
      <>
        Utilizes <a href="https://fs2.io" target="_blank">FS2</a> for purely functional, effectful, and polymorphic stream
        processing. You get full backpressure and streaming support out of the box.
      </>
    ),
  },
];

function Feature({imageUrl, title, description}) {
  const imgUrl = useBaseUrl(imageUrl);
  return (
    <div className={clsx('col col--4', styles.feature)}>
      {imgUrl && (
        <div className="text--center">
          <img className={styles.featureImage} src={imgUrl} alt={title} />
        </div>
      )}
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
}

function Home() {
  const context = useDocusaurusContext();
  const {siteConfig = {}} = context;
  return (
    <Layout
      description={siteConfig.tagline}>
      <header className={clsx('hero hero--primary', styles.heroBanner)}>
        <div className="container">
          <h1 className="hero__title">{siteConfig.title}</h1>
          <p className="hero__subtitle">{siteConfig.tagline}</p>
          <div className={styles.buttons}>
            <Link
              className={clsx(styles.indexGetStartedButton)}
              to={useBaseUrl('docs/')}>
              Get Started
            </Link>
          </div>
        </div>
      </header>
      <main>
        {features && features.length > 0 && (
          <section className={styles.features}>
            <div className="container">
              <div className="row">
                {features.map((props, idx) => (
                  <Feature key={idx} {...props} />
                ))}
              </div>
            </div>
          </section>
        )}
      </main>
    </Layout>
  );
}

export default Home;
