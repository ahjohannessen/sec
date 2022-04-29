(window.webpackJsonp=window.webpackJsonp||[]).push([[8],{75:function(e,t,n){"use strict";n.r(t),n.d(t,"frontMatter",(function(){return i})),n.d(t,"metadata",(function(){return o})),n.d(t,"toc",(function(){return l})),n.d(t,"default",(function(){return b}));var r=n(3),a=n(7),c=(n(0),n(86)),i={id:"intro",title:"Overview",sidebar_label:"Overview",slug:"/"},o={unversionedId:"intro",id:"intro",isDocsHomePage:!1,title:"Overview",description:"- sec is a EventStoreDB v20.10+ client library for Scala.",source:"@site/../sec-docs/target/mdoc/intro.md",slug:"/",permalink:"/sec/docs/",version:"current",sidebar_label:"Overview",sidebar:"mainSidebar",next:{title:"Quick Start",permalink:"/sec/docs/setup"}},l=[{value:"Using sec",id:"using-sec",children:[]},{value:"How to learn",id:"how-to-learn",children:[]},{value:"License",id:"license",children:[]}],s={toc:l};function b(e){var t=e.components,n=Object(a.a)(e,["components"]);return Object(c.b)("wrapper",Object(r.a)({},s,n,{components:t,mdxType:"MDXLayout"}),Object(c.b)("ul",null,Object(c.b)("li",{parentName:"ul"},"sec is a ",Object(c.b)("a",{parentName:"li",href:"https://www.eventstore.com"},"EventStoreDB v20.10+")," client library for Scala."),Object(c.b)("li",{parentName:"ul"},"sec is purely functional, non-blocking, and provides a tagless-final API."),Object(c.b)("li",{parentName:"ul"},"sec embraces the ",Object(c.b)("a",{parentName:"li",href:"https://www.scala-lang.org/conduct"},"Scala Code of Conduct"),".")),Object(c.b)("h3",{id:"using-sec"},"Using sec"),Object(c.b)("p",null,"To use sec in an existing ",Object(c.b)("a",{parentName:"p",href:"https://www.scala-sbt.org"},"sbt")," project with Scala 2.13 or a later version,\nadd the following to your ",Object(c.b)("inlineCode",{parentName:"p"},"build.sbt")," file."),Object(c.b)("pre",null,Object(c.b)("code",{parentName:"pre",className:"language-scala"},'libraryDependencies += "io.github.ahjohannessen" %% "sec-fs2-client" % "0.20.5"\n')),Object(c.b)("h3",{id:"how-to-learn"},"How to learn"),Object(c.b)("p",null,"In order to use sec effectively you need some prerequisites:"),Object(c.b)("ul",null,Object(c.b)("li",{parentName:"ul"},"It is assumed you are comfortable with ",Object(c.b)("a",{parentName:"li",href:"https://www.eventstore.com"},"EventStoreDB"),"."),Object(c.b)("li",{parentName:"ul"},"It is assumed you are comfortable with ",Object(c.b)("a",{parentName:"li",href:"https://typelevel.org/cats"},"cats"),", ",Object(c.b)("a",{parentName:"li",href:"https://typelevel.org/cats-effect"},"cats-effect"),",\nand ",Object(c.b)("a",{parentName:"li",href:"https://fs2.io"},"fs2"),".")),Object(c.b)("p",null,"If you feel not having the necessary prerequisites, the linked websites have many learning resources."),Object(c.b)("h4",{id:"learning-about-sec"},"Learning about sec"),Object(c.b)("p",null,"In the following sections you will learn about:"),Object(c.b)("ul",null,Object(c.b)("li",{parentName:"ul"},"Basics about ",Object(c.b)("a",{parentName:"li",href:"/sec/docs/types"},"types")," and ",Object(c.b)("a",{parentName:"li",href:"/sec/docs/client_api"},"API")," used for interacting with EventStoreDB."),Object(c.b)("li",{parentName:"ul"},"Using the ",Object(c.b)("strong",{parentName:"li"},"Streams API")," for ",Object(c.b)("a",{parentName:"li",href:"/sec/docs/writing"},"writing")," data, ",Object(c.b)("a",{parentName:"li",href:"/sec/docs/reading"},"reading")," data, ",Object(c.b)("a",{parentName:"li",href:"/sec/docs/subscribing"},"subscribing")," to streams,\n",Object(c.b)("a",{parentName:"li",href:"/sec/docs/deleting"},"deleting")," data. Moreover, you will also learn about manipulating ",Object(c.b)("a",{parentName:"li",href:"/sec/docs/metastreams"},"metadata"),"."),Object(c.b)("li",{parentName:"ul"},"Connecting to a ",Object(c.b)("a",{parentName:"li",href:"/sec/docs/config#single-node"},"single node")," or a ",Object(c.b)("a",{parentName:"li",href:"/sec/docs/config#cluster"},"cluster"),".  "),Object(c.b)("li",{parentName:"ul"},"Various ",Object(c.b)("a",{parentName:"li",href:"/sec/docs/config"},"configuration")," for connections, retries and authentication.  ")),Object(c.b)("h3",{id:"license"},"License"),Object(c.b)("p",null,"sec is licensed under ",Object(c.b)("a",{parentName:"p",href:"https://github.com/ahjohannessen/sec/blob/master/LICENSE"},"Apache 2.0"),"."))}b.isMDXComponent=!0},86:function(e,t,n){"use strict";n.d(t,"a",(function(){return u})),n.d(t,"b",(function(){return d}));var r=n(0),a=n.n(r);function c(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){c(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},c=Object.keys(e);for(r=0;r<c.length;r++)n=c[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var c=Object.getOwnPropertySymbols(e);for(r=0;r<c.length;r++)n=c[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var s=a.a.createContext({}),b=function(e){var t=a.a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},u=function(e){var t=b(e.components);return a.a.createElement(s.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return a.a.createElement(a.a.Fragment,{},t)}},f=a.a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,c=e.originalType,i=e.parentName,s=l(e,["components","mdxType","originalType","parentName"]),u=b(n),f=r,d=u["".concat(i,".").concat(f)]||u[f]||p[f]||c;return n?a.a.createElement(d,o(o({ref:t},s),{},{components:n})):a.a.createElement(d,o({ref:t},s))}));function d(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var c=n.length,i=new Array(c);i[0]=f;var o={};for(var l in t)hasOwnProperty.call(t,l)&&(o[l]=t[l]);o.originalType=e,o.mdxType="string"==typeof e?e:r,i[1]=o;for(var s=2;s<c;s++)i[s]=n[s];return a.a.createElement.apply(null,i)}return a.a.createElement.apply(null,n)}f.displayName="MDXCreateElement"}}]);