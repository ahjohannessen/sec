"use strict";(self.webpackChunksec=self.webpackChunksec||[]).push([[491],{5680:(e,t,n)=>{n.d(t,{xA:()=>c,yg:()=>f});var r=n(6540);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function s(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var u=r.createContext({}),l=function(e){var t=r.useContext(u),n=t;return e&&(n="function"==typeof e?e(t):s(s({},t),e)),n},c=function(e){var t=l(e.components);return r.createElement(u.Provider,{value:t},e.children)},p="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},y=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,u=e.parentName,c=i(e,["components","mdxType","originalType","parentName"]),p=l(n),y=o,f=p["".concat(u,".").concat(y)]||p[y]||d[y]||a;return n?r.createElement(f,s(s({ref:t},c),{},{components:n})):r.createElement(f,s({ref:t},c))}));function f(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,s=new Array(a);s[0]=y;var i={};for(var u in t)hasOwnProperty.call(t,u)&&(i[u]=t[u]);i.originalType=e,i[p]="string"==typeof e?e:o,s[1]=i;for(var l=2;l<a;l++)s[l]=n[l];return r.createElement.apply(null,s)}return r.createElement.apply(null,n)}y.displayName="MDXCreateElement"},5175:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>u,contentTitle:()=>s,default:()=>d,frontMatter:()=>a,metadata:()=>i,toc:()=>l});var r=n(8168),o=(n(6540),n(5680));const a={id:"setup",title:"Quick Start",sidebar_label:"Quick Start"},s=void 0,i={unversionedId:"setup",id:"setup",title:"Quick Start",description:"In order to learn about sec in the following sections you will need a basic setup.",source:"@site/../sec-docs/target/mdoc/setup.md",sourceDirName:".",slug:"/setup",permalink:"/sec/docs/setup",draft:!1,tags:[],version:"current",frontMatter:{id:"setup",title:"Quick Start",sidebar_label:"Quick Start"},sidebar:"mainSidebar",previous:{title:"Overview",permalink:"/sec/docs/"},next:{title:"Data Types",permalink:"/sec/docs/types"}},u={},l=[{value:"KurrentDB Setup",id:"kurrentdb-setup",level:3},{value:"Scala Setup",id:"scala-setup",level:3},{value:"Verify Your Setup",id:"verify-your-setup",level:3}],c={toc:l},p="wrapper";function d(e){let{components:t,...n}=e;return(0,o.yg)(p,(0,r.A)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,o.yg)("p",null,"In order to learn about sec in the following sections you will need a basic setup.\nBelow are instructions to get you up and running. "),(0,o.yg)("h3",{id:"kurrentdb-setup"},"KurrentDB Setup"),(0,o.yg)("p",null,"First we need to get an KurrentDB node loaded up. One easy way to do this is to use docker and start up a single\nnode in in-secure mode as follows:"),(0,o.yg)("pre",null,(0,o.yg)("code",{parentName:"pre",className:"language-console"},"docker run -it --rm --name es-node \\\n-p 2113:2113 \\\n-e KURRENTDB_MEM_DB=True \\\n-e KURRENTDB_INSECURE=True \\\n-e KURRENTDB_GOSSIP_ON_SINGLE_NODE=True \\\n-e KURRENTDB_DISCOVER_VIA_DNS=False \\\n-e KURRENTDB_START_STANDARD_PROJECTIONS=True \\\ndocker.cloudsmith.io/eventstore/kurrent-latest/kurrentdb:25.0.0\n")),(0,o.yg)("p",null,"The above is enough to get something up and running for the purpose of exploration and learning. Consult the\nKurrentDB ",(0,o.yg)("a",{parentName:"p",href:"https://docs.kurrent.io/server/v25.0/configuration"},"docs")," for more details on\nconfiguration for production setup."),(0,o.yg)("h3",{id:"scala-setup"},"Scala Setup"),(0,o.yg)("p",null,"Create a new project with sec as dependency:"),(0,o.yg)("pre",null,(0,o.yg)("code",{parentName:"pre",className:"language-scala"},'libraryDependencies += "io.github.ahjohannessen" %% "sec-fs2" % "0.43.0"\n')),(0,o.yg)("h3",{id:"verify-your-setup"},"Verify Your Setup"),(0,o.yg)("p",null,"In order to verify that you can reach the database try out the\nfollowing ",(0,o.yg)("a",{parentName:"p",href:"https://typelevel.org/cats-effect/datatypes/ioapp.html"},"IOApp"),":"),(0,o.yg)("pre",null,(0,o.yg)("code",{parentName:"pre",className:"language-scala"},'import cats.effect.*\nimport sec.api.*\n\nobject HelloWorld extends IOApp:\n\n  def run(args: List[String]): IO[ExitCode] = EsClient\n    .singleNode[IO](Endpoint("127.0.0.1", 2113))\n    .resource.use(client => \n      client.gossip.read.map(_.render).flatMap(str => IO(println(str)))\n    )\n    .as(ExitCode.Success)\n')),(0,o.yg)("p",null,"The methods used on ",(0,o.yg)("inlineCode",{parentName:"p"},"EsClient")," are explained in the subsequent sections."))}d.isMDXComponent=!0}}]);