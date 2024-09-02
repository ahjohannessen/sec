"use strict";(self.webpackChunksec=self.webpackChunksec||[]).push([[298],{5680:(e,t,n)=>{n.d(t,{xA:()=>d,yg:()=>c});var a=n(6540);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function r(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,i=function(e,t){if(null==e)return{};var n,a,i={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var p=a.createContext({}),l=function(e){var t=a.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):r(r({},t),e)),n},d=function(e){var t=l(e.components);return a.createElement(p.Provider,{value:t},e.children)},m="mdxType",y={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},g=a.forwardRef((function(e,t){var n=e.components,i=e.mdxType,o=e.originalType,p=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),m=l(n),g=i,c=m["".concat(p,".").concat(g)]||m[g]||y[g]||o;return n?a.createElement(c,r(r({ref:t},d),{},{components:n})):a.createElement(c,r({ref:t},d))}));function c(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var o=n.length,r=new Array(o);r[0]=g;var s={};for(var p in t)hasOwnProperty.call(t,p)&&(s[p]=t[p]);s.originalType=e,s[m]="string"==typeof e?e:i,r[1]=s;for(var l=2;l<o;l++)r[l]=n[l];return a.createElement.apply(null,r)}return a.createElement.apply(null,n)}g.displayName="MDXCreateElement"},203:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>p,contentTitle:()=>r,default:()=>y,frontMatter:()=>o,metadata:()=>s,toc:()=>l});var a=n(8168),i=(n(6540),n(5680));const o={id:"types",title:"Basic Data Types",sidebar_label:"Data Types"},r=void 0,s={unversionedId:"types",id:"types",title:"Basic Data Types",description:"In sec some basic data types surface in many use cases when interacting with EventStoreDB. In order to establish some basics",source:"@site/../sec-docs/target/mdoc/types.md",sourceDirName:".",slug:"/types",permalink:"/sec/docs/types",draft:!1,tags:[],version:"current",frontMatter:{id:"types",title:"Basic Data Types",sidebar_label:"Data Types"},sidebar:"mainSidebar",previous:{title:"Quick Start",permalink:"/sec/docs/setup"},next:{title:"Client API",permalink:"/sec/docs/client_api"}},p={},l=[{value:"StreamId",id:"streamid",level:3},{value:"StreamPosition",id:"streamposition",level:3},{value:"LogPosition",id:"logposition",level:3},{value:"A note on <code>Long</code> usage in positions",id:"a-note-on-long-usage-in-positions",level:3},{value:"StreamState",id:"streamstate",level:3},{value:"EventData",id:"eventdata",level:3},{value:"EventType",id:"eventtype",level:4},{value:"EventId",id:"eventid",level:4},{value:"Data",id:"data",level:4},{value:"Metadata",id:"metadata",level:4},{value:"ContentType",id:"contenttype",level:4},{value:"Event",id:"event",level:3}],d={toc:l},m="wrapper";function y(e){let{components:t,...n}=e;return(0,i.yg)(m,(0,a.A)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,i.yg)("p",null,"In sec some basic data types surface in many use cases when interacting with EventStoreDB. In order to establish some basics\nthese are described in the following - More details about the particulars of these types are in API documentation.  "),(0,i.yg)("h3",{id:"streamid"},"StreamId"),(0,i.yg)("p",null,"Streams in EventStoreDB have stream identifiers that can be classified as user defined and system defined.\nIn EventStoreDB streams prefixed with ",(0,i.yg)("inlineCode",{parentName:"p"},"$")," are reserved for the system, for instance ",(0,i.yg)("inlineCode",{parentName:"p"},"$settings"),".\nFurthermore, EventStoreDB also has a concept of metadata streams for streams. Metadata streams for system streams are prefixed\nwith ",(0,i.yg)("inlineCode",{parentName:"p"},"$$"),", e.g. the corresponding metadata stream for ",(0,i.yg)("inlineCode",{parentName:"p"},"$settings")," is ",(0,i.yg)("inlineCode",{parentName:"p"},"$$$settings"),". Metadata streams for user defined\nstreams are prefixed with a ",(0,i.yg)("inlineCode",{parentName:"p"},"$"),", e.g. ",(0,i.yg)("inlineCode",{parentName:"p"},"user_stream")," has a corresponding metadata stream named ",(0,i.yg)("inlineCode",{parentName:"p"},"$user_stream"),"."),(0,i.yg)("p",null,"In sec a stream identifier is an ADT called ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamId")," with variants ",(0,i.yg)("inlineCode",{parentName:"p"},"Id")," and ",(0,i.yg)("inlineCode",{parentName:"p"},"MetaId")," - ",(0,i.yg)("inlineCode",{parentName:"p"},"Id"),"\nhas two variants, ",(0,i.yg)("inlineCode",{parentName:"p"},"Normal")," and ",(0,i.yg)("inlineCode",{parentName:"p"},"System"),". The stream identifiers that a user can create are ",(0,i.yg)("inlineCode",{parentName:"p"},"Normal")," and ",(0,i.yg)("inlineCode",{parentName:"p"},"System"),",\nthis is done with ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamId.apply")," that returns an ",(0,i.yg)("inlineCode",{parentName:"p"},"Either[InvalidInput, Id]"),". Some examples of ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamId")," construction\nare:"),(0,i.yg)("pre",null,(0,i.yg)("code",{parentName:"pre",className:"language-scala"},'import sec.StreamId\n\nval user   = StreamId("user_stream")    // Right(Normal("user_stream")\nval system = StreamId("$system_stream") // Right(System("system_stream"))\n\n// MetaId for above you get from the metaId method.\n// The render method displays the stream identifier as EventStoreDB sees it.\n\nuser.map(_.metaId.render)   // Right("$$user_stream")\nsystem.map(_.metaId.render) // Right("$$$system_stream")\n\n// Invalid stream identifiers\n\nStreamId("")        // Left(InvalidInput("id cannot be empty"))\nStreamId("$$oops")  // Left(InvalidInput("value must not start with $$, but is $$oops"))\n')),(0,i.yg)("p",null,"Moreover, a few common system stream identififiers are located in the ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamId")," companion object, for instance:"),(0,i.yg)("pre",null,(0,i.yg)("code",{parentName:"pre",className:"language-scala"},"import sec.StreamId\n\nStreamId.All.render               // $all\nStreamId.Scavenges.render         // $scavenges\nStreamId.All.metaId.render        // $$$all\nStreamId.Scavenges.metaId.render  // $$$scavenges\n")),(0,i.yg)("h3",{id:"streamposition"},"StreamPosition"),(0,i.yg)("p",null,"When you store an event in EventStoreDB it is assigned a ",(0,i.yg)("strong",{parentName:"p"},"stream position")," in the individual stream it belongs to.\nIn sec a stream position is an ADT called ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamPosition")," with two variants, ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamPosition.Exact")," that contains\na ",(0,i.yg)("inlineCode",{parentName:"p"},"Long")," and ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamPosition.End")," that is an object representing the end of a stream."),(0,i.yg)("p",null,"A ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamPosition")," that you can create is ",(0,i.yg)("inlineCode",{parentName:"p"},"Exact")," and this is done with ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamPosition.apply")," that returns an\n",(0,i.yg)("inlineCode",{parentName:"p"},"Exact"),". Examples of ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamPosition")," construction:"),(0,i.yg)("pre",null,(0,i.yg)("code",{parentName:"pre",className:"language-scala"},"import sec.StreamPosition\n\nStreamPosition.Start // Exact(0)\nStreamPosition(1L)   // Exact(1)\nStreamPosition.End   // End\n")),(0,i.yg)("p",null,"One use case where you need to construct a ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamPosition")," is when you to store a pointer of the last processed\nevent for a particular stream as a ",(0,i.yg)("inlineCode",{parentName:"p"},"Long")," in a read model and, e.g. after a restart of your application,\nneed to resume reading from EventStoreDB."),(0,i.yg)("h3",{id:"logposition"},"LogPosition"),(0,i.yg)("p",null,"All events in EventStoreDB have a logical position in the global transaction log. A logical position consists of a ",(0,i.yg)("em",{parentName:"p"},"commit position")," value\nand a ",(0,i.yg)("em",{parentName:"p"},"prepare position")," value. In sec this is modelled as an ADT called ",(0,i.yg)("inlineCode",{parentName:"p"},"LogPosition")," with two variants, ",(0,i.yg)("inlineCode",{parentName:"p"},"LogPosition.Exact"),"\nthat contains two ",(0,i.yg)("inlineCode",{parentName:"p"},"Long")," values and ",(0,i.yg)("inlineCode",{parentName:"p"},"LogPosition.End")," representing the end of the log."),(0,i.yg)("p",null,"A ",(0,i.yg)("inlineCode",{parentName:"p"},"LogPosition")," that you can create from ",(0,i.yg)("inlineCode",{parentName:"p"},"Long")," values is ",(0,i.yg)("inlineCode",{parentName:"p"},"Exact"),", this is done with ",(0,i.yg)("inlineCode",{parentName:"p"},"LogPosition.apply")," that returns an\n",(0,i.yg)("inlineCode",{parentName:"p"},"Either[InvalidInput, Exact]"),". Examples of ",(0,i.yg)("inlineCode",{parentName:"p"},"LogPosition")," construction:"),(0,i.yg)("pre",null,(0,i.yg)("code",{parentName:"pre",className:"language-scala"},'import sec.LogPosition\n\nLogPosition.Start    // Exact(0, 0)\nLogPosition.End      // End\nLogPosition(1L, 1L)  // Right(Exact(1, 1))\nLogPosition(0L, 1L)  // Left(InvalidInput("commit must be >= prepare, but 0 < 1"))\n')),(0,i.yg)("p",null,"Cases where you construct a ",(0,i.yg)("inlineCode",{parentName:"p"},"LogPosition")," is similar to that of ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamPosition"),", maintaining a pointer of last\nprocessed event. However, here you keep a pointer to the global log, ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamId.All"),", instead of an individual\nstream."),(0,i.yg)("h3",{id:"a-note-on-long-usage-in-positions"},"A note on ",(0,i.yg)("inlineCode",{parentName:"h3"},"Long")," usage in positions"),(0,i.yg)("p",null,"EventStoreDB uses ",(0,i.yg)("inlineCode",{parentName:"p"},"uint64")," that Java does not have a corresponding type for. In order to work around that fact sec has a\ntype called ",(0,i.yg)("inlineCode",{parentName:"p"},"ULong")," that is able to represent ",(0,i.yg)("inlineCode",{parentName:"p"},"uint64")," by wrapping a regular ",(0,i.yg)("inlineCode",{parentName:"p"},"Long")," and use this for ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamPosition.Exact"),"\nand ",(0,i.yg)("inlineCode",{parentName:"p"},"LogPosition.Exact"),". As sec provides an instance of ",(0,i.yg)("inlineCode",{parentName:"p"},"cats.Order")," for ",(0,i.yg)("inlineCode",{parentName:"p"},"ULong")," it is possible to compare positions larger than ",(0,i.yg)("inlineCode",{parentName:"p"},"Long.MaxValue")," as well as positions in ",(0,i.yg)("inlineCode",{parentName:"p"},"[0, Long.MaxValue]"),"."),(0,i.yg)("p",null,"Moreover, this means that you can store a ",(0,i.yg)("inlineCode",{parentName:"p"},"LogPosition.Exact")," pointer for your read model by using ",(0,i.yg)("inlineCode",{parentName:"p"},"toLong")," on its ",(0,i.yg)("inlineCode",{parentName:"p"},"ULong")," values. The ",(0,i.yg)("inlineCode",{parentName:"p"},"ULong.toLong")," method might yield negative values, this is fine as when ",(0,i.yg)("inlineCode",{parentName:"p"},"LogPosition.Exact")," is constructed again it has an ",(0,i.yg)("inlineCode",{parentName:"p"},"cats.Order")," instance that works for all numbers in ",(0,i.yg)("inlineCode",{parentName:"p"},"uint64"),"."),(0,i.yg)("h3",{id:"streamstate"},"StreamState"),(0,i.yg)("p",null,"Some operations, such as appending events to a stream, require that you provide an ",(0,i.yg)("em",{parentName:"p"},"expectation")," of what ",(0,i.yg)("strong",{parentName:"p"},"state"),"\nthe stream currently is in. If the stream state does not fullfil that expectation then an exception will be raised by EventStoreDB.\nIn sec this expected stream state is represented by the ADT ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamState")," that has four variants:"),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("inlineCode",{parentName:"li"},"NoStream")," - The stream does not exist yet. "),(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("inlineCode",{parentName:"li"},"Any")," - No expectation about the current state of the stream."),(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("inlineCode",{parentName:"li"},"StreamExists")," - The stream, or its metadata stream, is present."),(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("inlineCode",{parentName:"li"},"StreamPosition.Exact")," - The stream exists and its last written stream position is expected to be ",(0,i.yg)("inlineCode",{parentName:"li"},"Exact"),".")),(0,i.yg)("p",null,"The ",(0,i.yg)("inlineCode",{parentName:"p"},"StreamState")," expectation can be used to implement optimistic concurrency. When you retrieve a stream from EventStoreDB,\nyou can take note of the current stream position, then when you append to the stream you can determine if the stream has\nbeen modified in the meantime."),(0,i.yg)("h3",{id:"eventdata"},"EventData"),(0,i.yg)("p",null,"The event data you store in EventStoreDB is composed of an event type, an event id, payload data, metadata and a content type.\nIn sec this is modelled as ",(0,i.yg)("inlineCode",{parentName:"p"},"EventData")," with types that are explained below."),(0,i.yg)("h4",{id:"eventtype"},"EventType"),(0,i.yg)("p",null,"An event type should be supplied for your event data. This is a unique string used to identify the type of event you are\nsaving. One might be tempted to use language runtime types for event types as it might make marshalling more convenient.\nHowever, this is not recommended as it couples storage to your types. Instead, you can use a mapping between event\ntypes stored in EventStoreDB and your concrete runtime types."),(0,i.yg)("p",null,"The string that EventStoreDB uses for the event type is modelled in sec as an ADT with two main variants\n",(0,i.yg)("inlineCode",{parentName:"p"},"Normal")," and ",(0,i.yg)("inlineCode",{parentName:"p"},"System"),". The type that you can create is ",(0,i.yg)("inlineCode",{parentName:"p"},"Normal")," and this is done with ",(0,i.yg)("inlineCode",{parentName:"p"},"EventType.apply"),"\nthat returns ",(0,i.yg)("inlineCode",{parentName:"p"},"Either[InvalidInput, Normal]"),". Input is validated for emptiness and not starting with ",(0,i.yg)("inlineCode",{parentName:"p"},"$")," that EventStoreDB\nuses for reserved system defined types such as ",(0,i.yg)("inlineCode",{parentName:"p"},"$>")," and ",(0,i.yg)("inlineCode",{parentName:"p"},"$metadata"),"."),(0,i.yg)("p",null,"Examples of ",(0,i.yg)("inlineCode",{parentName:"p"},"EventType")," construction:"),(0,i.yg)("pre",null,(0,i.yg)("code",{parentName:"pre",className:"language-scala"},'import sec.EventType\n\nEventType("foo.bar.baz") // Right(Normal("foo.bar.baz")\nEventType("")            // Left(InvalidInput("Event type name cannot be empty"))\nEventType("$@")          // Left(InvalidInput("value must not start with $, but is $@"))\n')),(0,i.yg)("p",null,"Common system types are located in the companion of ",(0,i.yg)("inlineCode",{parentName:"p"},"EventType"),", some examples are:"),(0,i.yg)("pre",null,(0,i.yg)("code",{parentName:"pre",className:"language-scala"},"import sec.EventType\n\nEventType.LinkTo.render          // $>\nEventType.StreamMetadata.render  // $metadata\nEventType.Settings.render        // $settings\nEventType.StreamReference.render // $@\n")),(0,i.yg)("h4",{id:"eventid"},"EventId"),(0,i.yg)("p",null,"The format of an event identifier is a ",(0,i.yg)("em",{parentName:"p"},(0,i.yg)("a",{parentName:"em",href:"https://en.wikipedia.org/wiki/Universally_unique_identifier"},"uuid"))," and is\nused by EventStoreDB to uniquely identify the event you are trying to append. If two events with the same uuid are appended to\nthe same stream in quick succession EventStoreDB only appends one copy of the event to the stream. More information about this\nis in the EventStoreDB ",(0,i.yg)("a",{parentName:"p",href:"https://eventstore.com/docs/dotnet-api/optimistic-concurrency-and-idempotence/index.html#idempotence"},"docs"),"\nabout concurrency and idempotence."),(0,i.yg)("h4",{id:"data"},"Data"),(0,i.yg)("p",null,"The ",(0,i.yg)("inlineCode",{parentName:"p"},"data")," field on ",(0,i.yg)("inlineCode",{parentName:"p"},"EventData")," is a ",(0,i.yg)("a",{parentName:"p",href:"https://github.com/scodec/scodec-bits"},"scodec")," ",(0,i.yg)("inlineCode",{parentName:"p"},"ByteVector")," encoded representation\nof your event data. If you store your data as JSON you can make use of EventStoreDB functionality for projections.\nHowever, it is also common to store data in a ",(0,i.yg)("a",{parentName:"p",href:"https://developers.google.com/protocol-buffers"},(0,i.yg)("inlineCode",{parentName:"a"},"protocol buffers"))," format."),(0,i.yg)("h4",{id:"metadata"},"Metadata"),(0,i.yg)("p",null,"It is common to store additional information along side your event data. This can be correlation id, timestamp, audit,\nmarshalling info and so on. EventStoreDB allows you to store a separate byte array containing this information to keep data\nand metadata separate. These extra bytes are stored in a ",(0,i.yg)("inlineCode",{parentName:"p"},"ByteVector")," field of ",(0,i.yg)("inlineCode",{parentName:"p"},"EventData")," called ",(0,i.yg)("inlineCode",{parentName:"p"},"metadata"),"."),(0,i.yg)("h4",{id:"contenttype"},"ContentType"),(0,i.yg)("p",null,"The ",(0,i.yg)("inlineCode",{parentName:"p"},"data")," and ",(0,i.yg)("inlineCode",{parentName:"p"},"metadata")," fields on ",(0,i.yg)("inlineCode",{parentName:"p"},"EventData")," have a content type, ",(0,i.yg)("inlineCode",{parentName:"p"},"ContentType"),", with\nvariants ",(0,i.yg)("inlineCode",{parentName:"p"},"Binary")," and ",(0,i.yg)("inlineCode",{parentName:"p"},"Json"),". This is used to provide EventStoreDB information about whether the data is stored as json or as\nbinary. As you might have noticed, both ",(0,i.yg)("inlineCode",{parentName:"p"},"ByteVector")," fields of ",(0,i.yg)("inlineCode",{parentName:"p"},"EventData")," share the same content type, this is because\nEventStoreDB does not support different content types for ",(0,i.yg)("inlineCode",{parentName:"p"},"data")," and ",(0,i.yg)("inlineCode",{parentName:"p"},"metadata"),"."),(0,i.yg)("p",null,"When ",(0,i.yg)("inlineCode",{parentName:"p"},"EventType")," is translated to the protocol of EventStoreDB it becomes ",(0,i.yg)("inlineCode",{parentName:"p"},"application/octet-stream")," for ",(0,i.yg)("inlineCode",{parentName:"p"},"Binary")," and\n",(0,i.yg)("inlineCode",{parentName:"p"},"application/json")," for ",(0,i.yg)("inlineCode",{parentName:"p"},"Json"),". In the future there might come more content types, e.g. something that corresponds to\n",(0,i.yg)("inlineCode",{parentName:"p"},"application/proto")," or ",(0,i.yg)("inlineCode",{parentName:"p"},"application/avro"),"."),(0,i.yg)("h3",{id:"event"},"Event"),(0,i.yg)("p",null,"Event data arriving from EventStoreDB either comes from an individual stream or from the global log. An event is encoded as an\n",(0,i.yg)("inlineCode",{parentName:"p"},"ADT")," called ",(0,i.yg)("inlineCode",{parentName:"p"},"Event")," that has the variants ",(0,i.yg)("inlineCode",{parentName:"p"},"EventRecord")," and ",(0,i.yg)("inlineCode",{parentName:"p"},"ResolvedEvent"),"."),(0,i.yg)("p",null,"An ",(0,i.yg)("inlineCode",{parentName:"p"},"EventRecord")," consists of the following data types:"),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("inlineCode",{parentName:"li"},"streamId: StreamId")," - The stream the event belongs to."),(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("inlineCode",{parentName:"li"},"streamPosition: StreamPosition.Exact")," - The stream position of the event in its stream."),(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("inlineCode",{parentName:"li"},"logPosition: LogPosition.Exact")," - The position of the event in the global stream."),(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("inlineCode",{parentName:"li"},"eventData: EventData")," - The data of the event."),(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("inlineCode",{parentName:"li"},"created: ZonedDateTime")," - The time the event was created.")),(0,i.yg)("p",null,"A ",(0,i.yg)("inlineCode",{parentName:"p"},"ResolvedEvent")," is used when consuming streams that link to other streams. It consists of:"),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("inlineCode",{parentName:"li"},"event: EventRecord")," - The linked event."),(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("inlineCode",{parentName:"li"},"link: EventRecord")," - The linking event record.")),(0,i.yg)("p",null,"See the API docs for various methods defined for ",(0,i.yg)("inlineCode",{parentName:"p"},"Event"),"."),(0,i.yg)("p",null,"Later on when using the ",(0,i.yg)("a",{parentName:"p",href:"/sec/docs/client_api"},"EsClient API"),", you will learn about reading from streams and instruct EventStoreDB to\nresolve links such that you get events of type ",(0,i.yg)("inlineCode",{parentName:"p"},"ResolvedEvent")," back."))}y.isMDXComponent=!0}}]);