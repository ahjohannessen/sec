(window.webpackJsonp=window.webpackJsonp||[]).push([[8],{63:function(e,t,n){"use strict";n.r(t),n.d(t,"frontMatter",(function(){return i})),n.d(t,"metadata",(function(){return o})),n.d(t,"rightToc",(function(){return s})),n.d(t,"default",(function(){return b}));var a=n(2),r=n(6),c=(n(0),n(74)),i={id:"intro",title:"Overview",sidebar_label:"Overview",slug:"/"},o={unversionedId:"intro",id:"intro",isDocsHomePage:!1,title:"Overview",description:"- sec is a EventStoreDB 20.6.x client library for Scala.",source:"@site/../sec-docs/target/mdoc/intro.md",slug:"/",permalink:"/sec/docs/",version:"current",sidebar_label:"Overview",sidebar:"mainSidebar",next:{title:"Quick Start",permalink:"/sec/docs/setup"}},s=[{value:"Using sec",id:"using-sec",children:[]},{value:"How to learn",id:"how-to-learn",children:[]},{value:"License",id:"license",children:[]}],l={rightToc:s};function b(e){var t=e.components,n=Object(r.a)(e,["components"]);return Object(c.b)("wrapper",Object(a.a)({},l,n,{components:t,mdxType:"MDXLayout"}),Object(c.b)("ul",null,Object(c.b)("li",{parentName:"ul"},"sec is a ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"https://www.eventstore.com"}),"EventStoreDB 20.6.x")," client library for Scala."),Object(c.b)("li",{parentName:"ul"},"sec is purely functional, non-blocking, and provides a tagless-final API."),Object(c.b)("li",{parentName:"ul"},"sec embraces the ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"https://www.scala-lang.org/conduct"}),"Scala Code of Conduct"),".")),Object(c.b)("h3",{id:"using-sec"},"Using sec"),Object(c.b)("p",null,"To use sec in an existing ",Object(c.b)("a",Object(a.a)({parentName:"p"},{href:"https://www.scala-sbt.org"}),"sbt")," project with Scala 2.13 or a later version,\nadd the following to your ",Object(c.b)("inlineCode",{parentName:"p"},"build.sbt")," file."),Object(c.b)("pre",null,Object(c.b)("code",Object(a.a)({parentName:"pre"},{className:"language-scala"}),'libraryDependencies += "io.github.ahjohannessen" % "sec-fs2" % "0.1.0-M8"\n')),Object(c.b)("p",null,"To use the latest published snapshot release, add the following lines to your ",Object(c.b)("inlineCode",{parentName:"p"},"build.sbt")," file."),Object(c.b)("pre",null,Object(c.b)("code",Object(a.a)({parentName:"pre"},{className:"language-scala"}),'resolvers           += Resolver.sonatypeRepo("snapshots")\nlibraryDependencies += "io.github.ahjohannessen" % "sec-fs2" % "0.1.0-M8+9-6f4fc69d-SNAPSHOT"\n')),Object(c.b)("h3",{id:"how-to-learn"},"How to learn"),Object(c.b)("p",null,"In order to use sec effectively you need some prerequisites:"),Object(c.b)("ul",null,Object(c.b)("li",{parentName:"ul"},"It is assumed you are comfortable with ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"https://www.eventstore.com"}),"EventStoreDB"),"."),Object(c.b)("li",{parentName:"ul"},"It is assumed you are comfortable with ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"https://typelevel.org/cats"}),"cats"),", ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"https://typelevel.org/cats-effect"}),"cats-effect"),",\nand ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"https://fs2.io"}),"fs2"),".")),Object(c.b)("p",null,"If you feel not having the necessary prerequisites, the linked websites have many learning resources."),Object(c.b)("h4",{id:"learning-about-sec"},"Learning about sec"),Object(c.b)("p",null,"In the following sections you will learn about:"),Object(c.b)("ul",null,Object(c.b)("li",{parentName:"ul"},"Basics about ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"/sec/docs/types"}),"types")," and ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"/sec/docs/client_api"}),"API")," used for interacting with EventStoreDB."),Object(c.b)("li",{parentName:"ul"},"Using the ",Object(c.b)("strong",{parentName:"li"},"Streams API")," for ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"/sec/docs/writing"}),"writing")," data, ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"/sec/docs/reading"}),"reading")," data, ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"/sec/docs/subscribing"}),"subscribing")," to streams,\n",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"/sec/docs/deleting"}),"deleting")," data. Moreover, you will also learn about manipulating ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"/sec/docs/metastreams"}),"metadata"),"."),Object(c.b)("li",{parentName:"ul"},"Connecting to a ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"/sec/docs/config#single-node"}),"single node")," or a ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"/sec/docs/config#cluster"}),"cluster"),".  "),Object(c.b)("li",{parentName:"ul"},"Various ",Object(c.b)("a",Object(a.a)({parentName:"li"},{href:"/sec/docs/config"}),"configuration")," for connections, retries and authentication.  ")),Object(c.b)("h3",{id:"license"},"License"),Object(c.b)("p",null,"sec is licensed under ",Object(c.b)("a",Object(a.a)({parentName:"p"},{href:"https://github.com/ahjohannessen/sec/blob/master/LICENSE"}),"Apache 2.0"),"."))}b.isMDXComponent=!0},74:function(e,t,n){"use strict";n.d(t,"a",(function(){return u})),n.d(t,"b",(function(){return O}));var a=n(0),r=n.n(a);function c(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){c(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},c=Object.keys(e);for(a=0;a<c.length;a++)n=c[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var c=Object.getOwnPropertySymbols(e);for(a=0;a<c.length;a++)n=c[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=r.a.createContext({}),b=function(e){var t=r.a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},u=function(e){var t=b(e.components);return r.a.createElement(l.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return r.a.createElement(r.a.Fragment,{},t)}},f=r.a.forwardRef((function(e,t){var n=e.components,a=e.mdxType,c=e.originalType,i=e.parentName,l=s(e,["components","mdxType","originalType","parentName"]),u=b(n),f=a,O=u["".concat(i,".").concat(f)]||u[f]||p[f]||c;return n?r.a.createElement(O,o(o({ref:t},l),{},{components:n})):r.a.createElement(O,o({ref:t},l))}));function O(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var c=n.length,i=new Array(c);i[0]=f;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o.mdxType="string"==typeof e?e:a,i[1]=o;for(var l=2;l<c;l++)i[l]=n[l];return r.a.createElement.apply(null,i)}return r.a.createElement.apply(null,n)}f.displayName="MDXCreateElement"}}]);