"use strict";(self.webpackChunksec=self.webpackChunksec||[]).push([[990],{3905:(e,t,r)=>{r.d(t,{Zo:()=>p,kt:()=>f});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var c=n.createContext({}),s=function(e){var t=n.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},p=function(e){var t=s(e.components);return n.createElement(c.Provider,{value:t},e.children)},m="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},d=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,c=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),m=s(r),d=a,f=m["".concat(c,".").concat(d)]||m[d]||u[d]||o;return r?n.createElement(f,i(i({ref:t},p),{},{components:r})):n.createElement(f,i({ref:t},p))}));function f(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,i=new Array(o);i[0]=d;var l={};for(var c in t)hasOwnProperty.call(t,c)&&(l[c]=t[c]);l.originalType=e,l[m]="string"==typeof e?e:a,i[1]=l;for(var s=2;s<o;s++)i[s]=r[s];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}d.displayName="MDXCreateElement"},924:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>c,contentTitle:()=>i,default:()=>u,frontMatter:()=>o,metadata:()=>l,toc:()=>s});var n=r(7462),a=(r(7294),r(3905));const o={id:"metastreams",title:"Metadata Streams",sidebar_label:"Metadata"},i=void 0,l={unversionedId:"metastreams",id:"metastreams",title:"Metadata Streams",description:"TODO:",source:"@site/../sec-docs/target/mdoc/metastreams.md",sourceDirName:".",slug:"/metastreams",permalink:"/sec/docs/metastreams",draft:!1,tags:[],version:"current",frontMatter:{id:"metastreams",title:"Metadata Streams",sidebar_label:"Metadata"},sidebar:"mainSidebar",previous:{title:"Deleting",permalink:"/sec/docs/deleting"}},c={},s=[],p={toc:s},m="wrapper";function u(e){let{components:t,...r}=e;return(0,a.kt)(m,(0,n.Z)({},p,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("p",null,"TODO: "),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"explain fine-grained API and types used: ",(0,a.kt)("inlineCode",{parentName:"li"},"MaxAge"),", ",(0,a.kt)("inlineCode",{parentName:"li"},"MaxCount"),", ",(0,a.kt)("inlineCode",{parentName:"li"},"CacheControl"),", ",(0,a.kt)("inlineCode",{parentName:"li"},"StreamPosition.Exact"),", ",(0,a.kt)("inlineCode",{parentName:"li"},"StreamAcl")," and ",(0,a.kt)("inlineCode",{parentName:"li"},"JsonObject"),"."),(0,a.kt)("li",{parentName:"ul"},"mention that ",(0,a.kt)("inlineCode",{parentName:"li"},"$")," is reserved for ESDB and what that entails for encoding/decoding custom metadata.")))}u.isMDXComponent=!0}}]);