"use strict";(self.webpackChunksec=self.webpackChunksec||[]).push([[56],{5680:(e,t,r)=>{r.d(t,{xA:()=>u,yg:()=>f});var n=r(6540);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var s=n.createContext({}),c=function(e){var t=n.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},u=function(e){var t=c(e.components);return n.createElement(s.Provider,{value:t},e.children)},p="mdxType",g={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},y=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,i=e.originalType,s=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),p=c(r),y=a,f=p["".concat(s,".").concat(y)]||p[y]||g[y]||i;return r?n.createElement(f,o(o({ref:t},u),{},{components:r})):n.createElement(f,o({ref:t},u))}));function f(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=r.length,o=new Array(i);o[0]=y;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[p]="string"==typeof e?e:a,o[1]=l;for(var c=2;c<i;c++)o[c]=r[c];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}y.displayName="MDXCreateElement"},3688:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>s,contentTitle:()=>o,default:()=>g,frontMatter:()=>i,metadata:()=>l,toc:()=>c});var n=r(8168),a=(r(6540),r(5680));const i={id:"intro",title:"Overview",sidebar_label:"Overview",slug:"/"},o=void 0,l={unversionedId:"intro",id:"intro",title:"Overview",description:"- sec is a EventStoreDB v22.10+ client library for Scala.",source:"@site/../sec-docs/target/mdoc/intro.md",sourceDirName:".",slug:"/",permalink:"/sec/docs/",draft:!1,tags:[],version:"current",frontMatter:{id:"intro",title:"Overview",sidebar_label:"Overview",slug:"/"},sidebar:"mainSidebar",next:{title:"Quick Start",permalink:"/sec/docs/setup"}},s={},c=[{value:"Using sec",id:"using-sec",level:3},{value:"How to learn",id:"how-to-learn",level:3},{value:"Learning about sec",id:"learning-about-sec",level:4},{value:"License",id:"license",level:3}],u={toc:c},p="wrapper";function g(e){let{components:t,...r}=e;return(0,a.yg)(p,(0,n.A)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,a.yg)("ul",null,(0,a.yg)("li",{parentName:"ul"},"sec is a ",(0,a.yg)("a",{parentName:"li",href:"https://www.eventstore.com"},"EventStoreDB v22.10+")," client library for Scala."),(0,a.yg)("li",{parentName:"ul"},"sec is purely functional, non-blocking, and provides a tagless-final API."),(0,a.yg)("li",{parentName:"ul"},"sec embraces the ",(0,a.yg)("a",{parentName:"li",href:"https://www.scala-lang.org/conduct"},"Scala Code of Conduct"),".")),(0,a.yg)("h3",{id:"using-sec"},"Using sec"),(0,a.yg)("p",null,"To use sec in an existing ",(0,a.yg)("a",{parentName:"p",href:"https://www.scala-sbt.org"},"sbt")," project with Scala 3 or a later version,\nadd the following to your ",(0,a.yg)("inlineCode",{parentName:"p"},"build.sbt")," file."),(0,a.yg)("pre",null,(0,a.yg)("code",{parentName:"pre",className:"language-scala"},'libraryDependencies += "io.github.ahjohannessen" %% "sec-fs2-client" % "0.41.3"\n')),(0,a.yg)("h3",{id:"how-to-learn"},"How to learn"),(0,a.yg)("p",null,"In order to use sec effectively you need some prerequisites:"),(0,a.yg)("ul",null,(0,a.yg)("li",{parentName:"ul"},"It is assumed you are comfortable with ",(0,a.yg)("a",{parentName:"li",href:"https://www.eventstore.com"},"EventStoreDB"),"."),(0,a.yg)("li",{parentName:"ul"},"It is assumed you are comfortable with ",(0,a.yg)("a",{parentName:"li",href:"https://typelevel.org/cats"},"cats"),", ",(0,a.yg)("a",{parentName:"li",href:"https://typelevel.org/cats-effect"},"cats-effect"),",\nand ",(0,a.yg)("a",{parentName:"li",href:"https://fs2.io"},"fs2"),".")),(0,a.yg)("p",null,"If you feel not having the necessary prerequisites, the linked websites have many learning resources."),(0,a.yg)("h4",{id:"learning-about-sec"},"Learning about sec"),(0,a.yg)("p",null,"In the following sections you will learn about:"),(0,a.yg)("ul",null,(0,a.yg)("li",{parentName:"ul"},"Basics about ",(0,a.yg)("a",{parentName:"li",href:"/sec/docs/types"},"types")," and ",(0,a.yg)("a",{parentName:"li",href:"/sec/docs/client_api"},"API")," used for interacting with EventStoreDB."),(0,a.yg)("li",{parentName:"ul"},"Using the ",(0,a.yg)("strong",{parentName:"li"},"Streams API")," for ",(0,a.yg)("a",{parentName:"li",href:"/sec/docs/writing"},"writing")," data, ",(0,a.yg)("a",{parentName:"li",href:"/sec/docs/reading"},"reading")," data, ",(0,a.yg)("a",{parentName:"li",href:"/sec/docs/subscribing"},"subscribing")," to streams,\n",(0,a.yg)("a",{parentName:"li",href:"/sec/docs/deleting"},"deleting")," data. Moreover, you will also learn about manipulating ",(0,a.yg)("a",{parentName:"li",href:"/sec/docs/metastreams"},"metadata"),"."),(0,a.yg)("li",{parentName:"ul"},"Connecting to a ",(0,a.yg)("a",{parentName:"li",href:"/sec/docs/config#single-node"},"single node")," or a ",(0,a.yg)("a",{parentName:"li",href:"/sec/docs/config#cluster"},"cluster"),".  "),(0,a.yg)("li",{parentName:"ul"},"Various ",(0,a.yg)("a",{parentName:"li",href:"/sec/docs/config"},"configuration")," for connections, retries and authentication.  ")),(0,a.yg)("h3",{id:"license"},"License"),(0,a.yg)("p",null,"sec is licensed under ",(0,a.yg)("a",{parentName:"p",href:"https://github.com/ahjohannessen/sec/blob/master/LICENSE"},"Apache 2.0"),"."))}g.isMDXComponent=!0}}]);