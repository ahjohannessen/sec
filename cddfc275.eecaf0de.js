(window.webpackJsonp=window.webpackJsonp||[]).push([[14],{82:function(e,t,n){"use strict";n.r(t),n.d(t,"frontMatter",(function(){return c})),n.d(t,"metadata",(function(){return i})),n.d(t,"toc",(function(){return s})),n.d(t,"default",(function(){return p}));var r=n(3),o=n(7),a=(n(0),n(86)),c={id:"setup",title:"Quick Start",sidebar_label:"Quick Start"},i={unversionedId:"setup",id:"setup",isDocsHomePage:!1,title:"Quick Start",description:"In order to learn about sec in the following sections you will need a basic setup.",source:"@site/../sec-docs/target/mdoc/setup.md",slug:"/setup",permalink:"/sec/docs/setup",version:"current",sidebar_label:"Quick Start",sidebar:"mainSidebar",previous:{title:"Overview",permalink:"/sec/docs/"},next:{title:"Basic Data Types",permalink:"/sec/docs/types"}},s=[{value:"EventStoreDB Setup",id:"eventstoredb-setup",children:[]},{value:"Scala Setup",id:"scala-setup",children:[]},{value:"Verify Your Setup",id:"verify-your-setup",children:[]}],u={toc:s};function p(e){var t=e.components,n=Object(o.a)(e,["components"]);return Object(a.b)("wrapper",Object(r.a)({},u,n,{components:t,mdxType:"MDXLayout"}),Object(a.b)("p",null,"In order to learn about sec in the following sections you will need a basic setup.\nBelow are instructions to get you up and running. "),Object(a.b)("h3",{id:"eventstoredb-setup"},"EventStoreDB Setup"),Object(a.b)("p",null,"First we need to get an EventStoreDB node loaded up. One easy way to do this is to use docker and start up a single\nnode in in-secure mode as follows:"),Object(a.b)("pre",null,Object(a.b)("code",{parentName:"pre",className:"language-console"},"docker run -it --rm --name es-node \\\n-p 2113:2113 \\\n-e EVENTSTORE_MEM_DB=True \\\n-e EVENTSTORE_INSECURE=True \\\n-e EVENTSTORE_GOSSIP_ON_SINGLE_NODE=True \\\n-e EVENTSTORE_DISCOVER_VIA_DNS=False \\\n-e EVENTSTORE_START_STANDARD_PROJECTIONS=True \\\neventstore/eventstore:21.10.2-bionic\n")),Object(a.b)("p",null,"The above is enough to get something up and running for the purpose of exploration and learning. Consult the\nEventStoreDB ",Object(a.b)("a",{parentName:"p",href:"https://developers.eventstore.com/server/v21.10/configuration.html#configuration-options"},"docs")," for more details on\nconfiguration for production setup."),Object(a.b)("h3",{id:"scala-setup"},"Scala Setup"),Object(a.b)("p",null,"Create a new project with sec as dependency:"),Object(a.b)("pre",null,Object(a.b)("code",{parentName:"pre",className:"language-scala"},'libraryDependencies += "io.github.ahjohannessen" %% "sec-fs2" % "0.20.10"\n')),Object(a.b)("h3",{id:"verify-your-setup"},"Verify Your Setup"),Object(a.b)("p",null,"In order to verify that you can reach the database try out the\nfollowing ",Object(a.b)("a",{parentName:"p",href:"https://typelevel.org/cats-effect/datatypes/ioapp.html"},"IOApp"),":"),Object(a.b)("pre",null,Object(a.b)("code",{parentName:"pre",className:"language-scala"},'import cats.effect._\nimport sec.api._\n\nobject HelloWorld extends IOApp {\n\n  def run(args: List[String]): IO[ExitCode] = EsClient\n    .singleNode[IO](Endpoint("127.0.0.1", 2113))\n    .resource.use(client => \n      client.gossip.read.map(_.render).flatMap(str => IO(println(str)))\n    )\n    .as(ExitCode.Success)\n\n}\n')),Object(a.b)("p",null,"The methods used on ",Object(a.b)("inlineCode",{parentName:"p"},"EsClient")," are explained in the subsequent sections."))}p.isMDXComponent=!0},86:function(e,t,n){"use strict";n.d(t,"a",(function(){return l})),n.d(t,"b",(function(){return f}));var r=n(0),o=n.n(r);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function c(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?c(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):c(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var u=o.a.createContext({}),p=function(e){var t=o.a.useContext(u),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},l=function(e){var t=p(e.components);return o.a.createElement(u.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return o.a.createElement(o.a.Fragment,{},t)}},b=o.a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,a=e.originalType,c=e.parentName,u=s(e,["components","mdxType","originalType","parentName"]),l=p(n),b=r,f=l["".concat(c,".").concat(b)]||l[b]||d[b]||a;return n?o.a.createElement(f,i(i({ref:t},u),{},{components:n})):o.a.createElement(f,i({ref:t},u))}));function f(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var a=n.length,c=new Array(a);c[0]=b;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i.mdxType="string"==typeof e?e:r,c[1]=i;for(var u=2;u<a;u++)c[u]=n[u];return o.a.createElement.apply(null,c)}return o.a.createElement.apply(null,n)}b.displayName="MDXCreateElement"}}]);