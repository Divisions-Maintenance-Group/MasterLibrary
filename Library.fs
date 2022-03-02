namespace MasterLibrary

module helpers = 
    let patch (list1: seq<'a>) (list2: seq<'a>) filterPredicate = 
        seq {list1; list2} 
        |> Seq.concat  
        |> Seq.filter filterPredicate
        |> Seq.distinct 
        |> Seq.toList

    let ifNull defaultValue value =
        value |> Option.ofObj |> Option.defaultValue defaultValue

module Framework = 
    open RocksDbSharp

    type UUID = private {
        Value: byte[]
    } with
        static member New () = 
            {Value = System.Guid.NewGuid().ToByteArray()}: UUID
        static member New (bytes: byte[]): Result<UUID, string> = 
            if bytes.Length = 16 then
                Ok({Value = bytes}: UUID)
            else
                Error "This byte array is not a valid UUID"
    
        static member TryParse (str: string): Option<UUID> = 
            try
                Some (UUID.Parse(str))
            with _ ->
                None
    
        static member private Parse (str: string): UUID = 
            let asPairs (source: seq<_>) =
                seq { 
                    use iter = source.GetEnumerator() 
                    while iter.MoveNext() do
                        let first = iter.Current
                        if iter.MoveNext() then
                            let second = iter.Current 
                            yield (first, second)
                }
            let charToByte (a:char) =
                match a with
                | '0' -> 0uy
                | '1' -> 1uy
                | '2' -> 2uy
                | '3' -> 3uy
                | '4' -> 4uy
                | '5' -> 5uy
                | '6' -> 6uy
                | '7' -> 7uy
                | '8' -> 8uy
                | '9' -> 9uy
                | 'a' -> 10uy
                | 'b' -> 11uy
                | 'c' -> 12uy
                | 'd' -> 13uy
                | 'e' -> 14uy
                | 'f' -> 15uy
                | _ -> failwith "The character you supplied can't be converted"
            let convertUuidStringToBytes uuidString = 
                uuidString 
                |> Seq.toList 
                |> asPairs
                |> Seq.map (fun (a, b) -> (charToByte a <<< 4) + charToByte b)  
                |> Seq.toArray
    
            let uuidString = str |> String.filter (fun i -> i <> '-')
            match System.Text.RegularExpressions.Regex.IsMatch(uuidString, "^[0-9a-f]{32}$") with
            | true -> {Value = uuidString |> convertUuidStringToBytes}: UUID
            | false -> invalidArg (nameof str) "This string is not a valid UUID"
        member this.GetBytes = 
            this.Value
        override this.ToString() = 
            this.GetString
        member this.GetString =
            let byteToChar (a:byte) =
                match a with
                | 0uy  -> '0'  
                | 1uy  -> '1'  
                | 2uy  -> '2'  
                | 3uy  -> '3'  
                | 4uy  -> '4'  
                | 5uy  -> '5'  
                | 6uy  -> '6'  
                | 7uy  -> '7'  
                | 8uy  -> '8'  
                | 9uy  -> '9'  
                | 10uy -> 'a'   
                | 11uy -> 'b'   
                | 12uy -> 'c'   
                | 13uy -> 'd'   
                | 14uy -> 'e'   
                | 15uy -> 'f'   
                | _ -> raise (System.Exception("The byte you supplied can't be converted"))
            this.Value 
            |> Seq.mapi 
                (fun counter i -> 
                    let a = (i &&& 0b11110000uy) >>> 4 |> byteToChar
                    let b = i &&& 0b00001111uy |> byteToChar
                    match counter with
                    | 4
                    | 6
                    | 8
                    | 10 -> $"-{a}{b}"
                    | _  -> $"{a}{b}"
                )
            |> String.concat ""


    type RocksIndex<'T when 'T :> Google.Protobuf.IMessage<'T>>(db: RocksDb, messageParser: Google.Protobuf.MessageParser<'T>, columnFamilyName: string) = 
        let cf = db.GetColumnFamily(columnFamilyName)

        member this.getStateOffset = 
            match db.Get($"{columnFamilyName}-state") with
            | null -> 0L
            | x -> int64 x
        
        member this.getEventOffset = 
            match db.Get($"{columnFamilyName}-event") with
            | null -> 0L
            | x -> int64 x

        member this.setStateOffset (offset: int64) = 
            db.Put($"{columnFamilyName}-state", offset.ToString())
        
        member this.setEventOffset (offset: int64) = 
            db.Put($"{columnFamilyName}-event", offset.ToString())

        member this.get (key: UUID): Option<'T> = 
            try 
                let key = key.GetBytes
                match db.Get(key, cf) with
                | null -> None
                | bytes -> Some (messageParser.ParseFrom(bytes))
            with e ->
                raise (System.Exception("You probably need to destroy your old rocksdb indexes (they are likely out of date)", e))
        member this.put (key: UUID) (value: Option<'T>): unit = 
            let pkey = key.GetBytes
            let returnValue = 
                match value with
                | Some x -> db.Remove(pkey, cf)
                            db.Put(pkey, Google.Protobuf.MessageExtensions.ToByteArray(x), cf)
                | None -> db.Remove(pkey, cf)
            returnValue

    module Kafka = 
        open Confluent.Kafka;
        open System.Net;

        type Kafka(bootstrapServers: string, topic: string) = 
            let bootstrapServers = bootstrapServers
            let topic = topic
            let pConfig = ProducerConfig()
            do
                pConfig.BootstrapServers <- bootstrapServers ///"host1:9092,host2:9092"
                pConfig.ClientId <- UUID.New().ToString()
            let producer = ProducerBuilder<byte[], byte[]>(pConfig).Build()

            member self.getCurrentHeadOffset() = 
                let cConfig = new ConsumerConfig()
                cConfig.BootstrapServers <- bootstrapServers//"host1:9092,host2:9092"
                cConfig.GroupId <- UUID.New().ToString()
                cConfig.AutoOffsetReset <- AutoOffsetReset.Earliest
                use consumer = ConsumerBuilder<byte[], byte[]>(cConfig).Build()
                consumer.Subscribe(topic)
                let current_offset_plus_one = (consumer.QueryWatermarkOffsets (Confluent.Kafka.TopicPartition(topic, 0), System.TimeSpan.FromMilliseconds(5000))).High.Value
                max 0L (current_offset_plus_one - 1L)

            member self.readWholeStream(lineProcessor: (UUID -> byte[] -> int64 -> unit)) = 
                let cConfig = new ConsumerConfig()
                cConfig.BootstrapServers <- bootstrapServers//"host1:9092,host2:9092"
                cConfig.GroupId <- UUID.New().ToString()
                cConfig.AutoOffsetReset <- AutoOffsetReset.Earliest
                use consumer = ConsumerBuilder<byte[], byte[]>(cConfig).Build()
                consumer.Subscribe(topic)
                let current_offset_plus_one = (consumer.QueryWatermarkOffsets (Confluent.Kafka.TopicPartition(topic, 0), System.TimeSpan.FromMilliseconds(5000))).High.Value
                let mutable result = consumer.Consume(5000)
                let mutable exit = false
                if result <> null then
                    while current_offset_plus_one - 1L >= result.Offset.Value && not exit do
                        let key = 
                            match (UUID.New result.Message.Key) with
                            | Ok(x) -> x | Error(x) -> failwith "UUID wasn't correct"
                        let value = result.Message.Value
                        let offset = result.Offset.Value
                        lineProcessor key value offset
                        if current_offset_plus_one - 1L > result.Offset.Value then 
                            result <- consumer.Consume()
                        else
                            exit <- true
                consumer.Close();

            member self.writeLineToStream(uuid: UUID, line: byte[]) = 
                async {
                    let uuidstuff = uuid.GetBytes
                    let! result = producer.ProduceAsync (
                        topic, 
                        new Message<byte[], byte[]>(Key = uuidstuff, Value = line)) |> Async.AwaitTask
                    return result.Offset.Value
                }
    
    module MockKafka =
        type MockKafka(path: string) =
            let path = path
    
            member self.readWholeStream(lineProcessor: (UUID -> string -> unit)) = 
                let lines = System.IO.File.ReadLines(path)
                for line in lines do
                    let index = line.IndexOf(',')
    
                    let uuidString = line.Substring(0, index)
                    let base64String = line.Substring(index + 1, line.Length - index - 1)
                    let uuid = UUID.TryParse(uuidString).Value
                    lineProcessor uuid base64String
            
            member self.writeLineToStream(uuid: UUID, line: string) = 
                let uuidstuff = uuid.ToString()
                System.IO.File.AppendAllText(path, $"{uuid.ToString()},{line}\n")

    type Event<'EventWrapper, 'Entity when 'EventWrapper :> Google.Protobuf.IMessage and 'Entity :> Google.Protobuf.IMessage> =
        abstract validate: Option<'Entity> -> Result<unit, seq<string>>
        abstract project: Option<'Entity> -> Option<'Entity>
        abstract addToEventWrapper: 'EventWrapper -> 'EventWrapper
    
    open System.Collections.Generic
    open System.Runtime.CompilerServices

    //TODO: change return value from true/false to Result and pass that back to the caller
    //TODO: put in some runtime checks that uuids from events are all the same
    let applyEvents<'EventWrapper, 'Entity when 'EventWrapper :> Google.Protobuf.IMessage and 'Entity :> Google.Protobuf.IMessage<'Entity>> 
        (wrapper: 'EventWrapper)
        (uuid: UUID)
        (events: seq<Event<'EventWrapper, 'Entity>>) 
        (index: RocksIndex<'Entity>) 
        (eventTopic: Kafka.Kafka) 
        (stateTopic: Kafka.Kafka) : Async<Result<int64 * int64, seq<string>>> = 
    
        async {
            let eventUuid = UUID.New();
            let (success, uuid, entity, wrappedEvents) = 
                try 
                    let first = events |> Seq.head
                    let state = index.get uuid
                    let wrappedEvents = wrapper
                    ((Ok(), uuid, state, wrappedEvents), events) ||> Seq.fold (fun (keepGoing, uuid, state, wrappedEvents) event ->
                        match keepGoing with
                        | Error(_) -> (keepGoing, uuid, state, wrappedEvents)
                        | Ok(_) -> 
                            let keepGoing = event.validate state
                            let entity = event.project(state)
                            let wrappedEvents = event.addToEventWrapper(wrappedEvents)
                            (keepGoing, uuid, entity, wrappedEvents)
                    )
                with e ->
                    printfn "%A" e
                    (Error( seq {"Error while processing (There is likely an error in the validation or projection logic)"}), uuid, None, Unchecked.defaultof<'EventWrapper>)
            match success with
            | Ok(_) ->
                let eventBytes = Google.Protobuf.MessageExtensions.ToByteArray(wrappedEvents)
                let stateBytes = 
                    match entity with
                    | None -> Array.zeroCreate 0
                    | Some(x) -> Google.Protobuf.MessageExtensions.ToByteArray(x)
                let! eventOffset = eventTopic.writeLineToStream(eventUuid, eventBytes)
                let! stateOffset = stateTopic.writeLineToStream(uuid, stateBytes)
                index.put uuid entity
                index.setEventOffset eventOffset
                index.setStateOffset stateOffset
                return Ok((eventOffset, stateOffset))
            | Error(failureMessages) ->
                return Error(failureMessages)
        }
    
    let rebuildIndex (index: RocksIndex<_>) (topic: Kafka.Kafka) (messageParser: Google.Protobuf.MessageParser<'Entity>) = 
        topic.readWholeStream 
            (
                fun uuid data offset -> 
                    if data |> Array.isEmpty then
                        index.put uuid None
                        index.setStateOffset offset
                    else
                        let entity = messageParser.ParseFrom(data);
                        index.put uuid (Some entity)
                        index.setStateOffset offset
            )