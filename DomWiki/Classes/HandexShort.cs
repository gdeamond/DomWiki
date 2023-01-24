/**
* @ Author: GDeamond
* @ Create Time: 2022-12-04 18:42:02
* @ Modified time: 2022-12-18 13:18:56
* @ Description: Class Handex organizes hash-table for storage and fast access to CONSTANT IMMUTABLE objects.
*       The principle of this table is similar to internment of immutable strings in .NET platform.
*       It can be helpful when required to work with enormously large amount of like-textual data.
*/

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
namespace DomWiki {
    [Serializable]
    internal class HandexShort {
 /** 
 *      HandexShort is short version of Handex.
 *      Handex is a value indexer (or interner) intended for reduce memory consumption of table data or dataset and could
 *  be used instead of Dictionary in this article: https://habr.com/ru/post/259069/.
 *      Why Handex?
 *      - unlike Dictionary the Handex uses less memory for infrastructure (https://habr.com/ru/post/488836/);
 *      - unlike simple Array of values the Handex provides fast operation of determining the presence of value in storage.
 *  
 *      The HandexShort has 4 bytes for addressing of value (instead of 8 bytes in Handex).
 *       
 *      Disadvantages of Handex:
 *          - objects are immutable (changed object must be re-added and all backlinks to be updated; object's old version
 *              still stays there);
 *          - if hash function is too bad the total number of elements may be unsatisfactory;
 *          - the maximum size is limited to 4G elements but in a reality it will never been achieved. 
 *
 *      There used strong array of rows (arrays) which consumes 9 bytes per element (64bit reference + 8bit signature)
 *  
 *      Since the search in unsorted collections is performed linearly, we would not want the rows be too large. Rows splits
 *  one big array into small subArrays - rows which will have their address calculated from hash function of the value.
 *  Then we just need to perform linear search in a small collection. Hash function must produce most uniform distribution.
 *  We selected xxHash function (https://github.com/uranium62/xxHash). Though we still use some hash-alignment trick - if
 *  during adding of value the row is full, we still try to add value in one of 3 other rows with addresses:
 *      1) ~hash (inversed bits)
 *      2) hash rotate left 12 mod 24
 *      3) ~(hash rotate left 12 mod 24)
 *  This means that a search of element should be performed in total of 4 rows.
 *      The address of the particular element consists of 2 parts:
 *          1st - index OF the row (hash result "handex", 10-24 bit),
 *          2nd - index INSIDE the row (which should not be too large, 8 bit).
 *      
 *      Each item has its calculated Handex and Signature generated with Pearson's hash function. Signatures actually used in
 *  Handex for speedup search of elements in the row as follows: row is selected by handex in one step. Then next step is a
 *  sequential search in a row.
 *      
 *      = Enlarging storage =
 *      The occupied memory can be increased in two ways: "horizontally" and "vertically". At the very start all rows has
 *  small capacity - less then threshold. As soos row reaches it's initialized size which is still less then threshold, this
 *  particular row grows up to it's next threshold size (which can be less than common threshold) - "horizontal" enlargement.
 *      As soos as at least one row reaches common threshold, the quantity of rows is increased ("vertical" enlargement).
 *  During "vertical" enlargement we expect the half of stored elements should move to the newly created rows.
 *      Since hashes are stored as full 32-bit values, it's recalculation is not required, but just extend hashMask and run
 *  through all rows for move some elements to new rows - all backlinks for moved elements still points to proper element:
 *          - only that half of elements required movement to other rows which has hash & threhold/2 > 0; example:
 *              hash = 11110101, storage bitWidth = 4, hence handex = 0101 and threshold = bitWidth^2 = 16; after enlargement
 *              the new handex will be 10101 (become +8), threshold = 32, handex & threshold/2 = 16, so this element should
 *              move to new row;
 *          - but need to place them to the same index which they had in old rows — so we provide proper work of backlink;
 *          - when element is moved to non-zero position of just created new row we have to track empty fields in that
 *              row - this is solved by organizing separate list of free indexes (what is unpleasant) for memory consumption:
 *              in worst case the storage will be increased twice in memory after enlargement.
 *      
 *      Two imprortant things:
 *      1) since Handex does not track backlinks, the only way to optimize Handex (to remove empty or non-used fields) is
 *          rebuild it from scratch;
 *      2) it is required to know the type of object you want to get from Handex (because Handex returns "nullable object").
 * 
 *      Realization of vertical enlargement.
 *      1 - block for write, open for read
 *      2 - make copy of storage (because of storage is arrays of arrays, the copy will have same rows as storage)
 *      3 - modify the copy
 *      4 - block for read and write, replace storages (storage = copy), open for read and write
 *      5 - GC.Collect(forced, last generation, blocking = false)
 *  
 *  + upgrade: provide mutability of stored objects
 *      There is possibility to provide mutability for stored objects. Will be need to create an additional reference-layer
 *  (another 8 bytes per item). This layer will function as a broker providing proper reference to the object and refreshing
 *  objects state inside the storage. All backlinks will be stored in reference layer what will consume additional 8 bytes
 *  per object - however it is still less than Generic Dictionary. But it will be required to call "update object" function
 *  for update internal reference between "broker" and actual field of the updated object, and also clear old reference.
 *      Disadvantages:
 *          - reduced access speed due to double referencing objects: instead of direct access to array
 *              (object=array[row][index]) there will be two steps
 *              (object=broker[address]:array[row][index]) though it can still be enough fast. SpeedTests required.
 *      Solution:
 *          - create super-class implementing or internally using the Handex
 * 
 *  + upgrade: provide key-value mapping for replace of Dictionary
 *      Solution:
 *          - create super-class implementing or internally using the Handex
 *
 *  + upgrade: provide thread-safe access to storage
 *      There are two locking layers. First - is main lock, used when required to enlarge storage size. Second - is row lock.
 *  Respectively there is StorageBroker and RowBroker objects that are used for thread synchronizing. When the thread wants to
 *  view/change value it requests Access from StorageBroker or/and Rowbroker. Broker checks if the row/storage is free and
 *  either grants access or waits timeout.
 *      When the storage is to be enlarged, StorageBroker requests WriteAccess from StorageBroker and RowBroker for all rows.
 *  After all rows marked as "locked" the RowBoker releases write access. While row is locked the request to RowBroker will
 *  return "denied access". Worker thread now works on enlarging of storage. After each row is updated the lock-status become
 *  released, and RowBroker may grant access to this row.
 *      To make this able we have to define third locking-layer: separate ReaderWriterLock for each row. But we want to use
 *  as less memory as possible - without creating object per each row. So we define the byte for each row which will store
 *  information about row status and count of "readers". We know that many readers and only one writer may access the row.
 *  So the byte-lock may be used as follows: most higher bit - is write lock status, 7 lower bits - count of readers.
 *  When the thread wants to write into the row, it sets "write lock status" - then no other readers can be added:
 *      - if (byteLock)<127 grantAccess(accessType: Read/Write); // if byteLock == 127 no other readers can be allocated;
 *      // if byteLock > 128 row is write locked but awaits when all readers will exit;
 *      // if byteLock == 128 row is write locked and can be writed.
 *  After all readers exits row, the thread performs write actions and then releases row: byteLock = 0.
 */
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        public struct ItemData {
            public uint Hash = 0; // 4 bytes
            public short Index = -1; // 2 bytes
            public byte Signature = 0; // 1 byte
            public Com Command = Com.none; // 1 byte

            public ItemData(){}
        }

        public struct CommandData {
            ItemData meta = new ItemData();
            object? value = null;

            public CommandData(){}
        }

        public enum Com : byte {
            none,
            Add,
            Remove
        }

        [NonSerialized] private const uint HANDEXSHORT_BIT_WIDTH = 10U; // default width of handex, rows quantity (4096 elements)
        [NonSerialized] private const uint HANDEXSHORT_BIT_WIDTH_MIN = 9U; // minimum width of handex (1024 elements)
        [NonSerialized] private const uint HANDEXSHORT_BIT_WIDTH_MAX = 24U; // maximum width of handex (2E+12 elements)
        private const uint HASHMASK = uint.MaxValue - 255; // left three bytes for hash, right 1 byte for index
        private TimeSpan WAITTIME = new TimeSpan(1L); // 100 nano-sec

        private uint bitWidth, threshold;
        private uint countElements = 0; // counts all elements
        private uint countEmpties = 0; // counts free indexes

        private object[][] values; // stores objects except field [0][0] - address of this field means "null"
        private byte[][] signatures; // stores Pearson's hashes
        private byte[] locks; // bytes indicating lock status of the row (7 bits - count of readers, 1 bit - write lock)
        private ConcurrentQueue<CommandData> commandBuffer = new ConcurrentQueue<CommandData>(); // this buffer is used to temporary contain operations on values to be processed with storage
       
        private List<byte>[] freexs; // stores free indexes for rows; indexes sorted in reverse order, so the first elemen in List contains maximum index value
        

        private ReaderWriterLockSlim storageBroker = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        private ReaderWriterLockSlim rowBroker = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

        public ulong Count { get => countElements; }
        public uint BitWidth { get => bitWidth; }
        private uint HashMask { get => bitWidth-1; }
        private uint Threshold(uint bitwidth) {
            if (bitwidth<10) return 2;
            return (bitwidth-8)*(bitwidth-8);
        }

#region #region StaticFunctions

        private static System.Data.HashFunction.Pearson.IPearson prs = System.Data.HashFunction.Pearson.PearsonFactory.Instance.Create();
        private static Random rand = new Random();

        /// <summary>
        /// Returns array {xxHash(), PearsonHash()} from object <paramref name="item" />
        /// </summary>
        /// <param name="item"></param>
        /// <returns>array: [0] - 32-bit hash, [1] - 8-bit hash</returns>
        /// v -----------------------------------------------------------------------------------------------------------------------------
        public static ItemData ComputeHash(object? item){ // returns [hash, Pearson's hash]
            uint hs = 0;
            byte hp = 0;
            
            if (item is not null) {
                /*
                *  To use BinaryFormatter (which is obsolete and not recommended to use), need next configuration in .csproj:
                *  <PropertyGroup>
                *      <!-- Warning: Setting the following switch is *NOT* recommended in web apps -->
                *      <EnableUnsafeBinaryFormatterSerialization>true</EnableUnsafeBinaryFormatterSerialization>
                *  </PropertyGroup>
                */
                System.Runtime.Serialization.Formatters.Binary.BinaryFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                using (System.IO.MemoryStream strm = new MemoryStream()){
                    #pragma warning disable SYSLIB0011 // disables warning message related to use of obsolete feature - BinaryFormatter
                        formatter.Serialize(strm, item!);
                    #pragma warning restore SYSLIB0011 // enables warning
                    
                    strm.Position = 0;
                    hs = GetHashFromStream(strm);
                    strm.Position = 0;
                    hp = prs.ComputeHash(strm).Hash[0];
                }
            }
            return new ItemData() with {Hash = hs, Signature = hp};
        }
        
        /// <summary>
        /// Returns array {xxHash(), PearsonHash()} from object <paramref name="item" />
        /// </summary>
        /// <param name="item"></param>
        /// <returns>array: [0] - 32-bit hash, [1] - 8-bit hash</returns>
        /// v -----------------------------------------------------------------------------------------------------------------------------
        public static ItemData ComputeHash(string item){ // returns [hash, Pearson's hash]
            return new ItemData() with {
                Hash = GetHashFromString(item),
                Signature = prs.ComputeHash(System.Text.Encoding.ASCII.GetBytes(item)).Hash[0]
            };
        }
        
        /// <summary>
        /// Returns xxHash() from string <paramref name="value" />
        /// </summary>
        /// <param name="value">string</param>
        /// <returns>uint hash value</returns>
        /// v -----------------------------------------------------------------------------------------------------------------------------
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint GetHashFromString(string value){
            return (Standart.Hash.xxHash.xxHash32.ComputeHash(value));
        }

        /// <summary>
        /// Returns xxHash() from byte stream <paramref name="strm" />
        /// </summary>
        /// <param name="strm">stream</param>
        /// <returns>uint hash value</returns>
        /// v -----------------------------------------------------------------------------------------------------------------------------
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint GetHashFromStream(Stream strm){
            return (Standart.Hash.xxHash.xxHash32.ComputeHash(strm));
        }

#endregion 


        private class LockEventArgs : EventArgs {
            public int LockNum { get; set; }

            public LockEventArgs(int lockNum) {
                LockNum = lockNum;
            }
        }

        private delegate void LockEventHandler(LockEventArgs args);
        private event LockEventHandler? LockChangedEvent;
        private event LockEventHandler? RowReleasedEvent;
        // after lock changed must call LockChangedEvent()
        // subscribe post-processing funcitons to LockChangedEvent


        /// <summary>
        /// Initializes array storage for indexed elements: the higher arrayBitWidth the faster searching of the element
        /// but the more memory will be consumed at start. Maximum arrayBitWidth is 24, minimum 9, default 10.
        /// </summary>
        /// <param name="arrayBitWidth">Sets the size of the top array and the width of hash codes</param>
        /// ? -----------------------------------------------------------------------------------------------------------------------------
        public HandexShort (int newBitWidth = (int)HANDEXSHORT_BIT_WIDTH){
            bitWidth = (uint)Math.Min(HANDEXSHORT_BIT_WIDTH_MAX, Math.Max(HANDEXSHORT_BIT_WIDTH_MIN, newBitWidth));

            threshold = Threshold(bitWidth); // threshold of the row size at which the storage will be enlarged
            int rowsCount = 2<<(int)(bitWidth-1); // = Math.Pow(2, bitWidth)

            // create new instance:
            values = new object[rowsCount][];
            signatures = new byte[rowsCount][];
            freexs = new List<byte>[rowsCount]; // element 0 keeps length of row
            locks = new byte[rowsCount];

            // init storage
            for (uint i=0; i<rowsCount; i++) initRow (i);
            
            // "add" null-object:
            save (null, 0, 0, 0); freexs[0][0]=1;

        }

        /// ? -----------------------------------------------------------------------------------------------------------------------------
        private void initRow(uint handex){
            uint thr = Threshold(HANDEXSHORT_BIT_WIDTH_MIN);
            values[handex] = new object[thr];
            signatures[handex] = new byte[thr];
            freexs[handex] = new List<byte>(){0}; // first element contains maximum free index
            locks[handex] = 0; // initial state of row locker
        }

        /// ? -----------------------------------------------------------------------------------------------------------------------------
        private void save(object? item, uint row, byte signature, byte index){
            // ensure row is exist and initialized for proper length, index should be < row length
            #pragma warning disable CS8601 // suppresses warning about "item" can be "null" 
            values[row]![index] = item;
            #pragma warning restore CS8601
            signatures[row]![index] = signature;
        }



        
        /// <summary>
        /// Awaits and provides required row lock status
        /// </summary>
        /// <param name="rowNum">The row number to wait for unlock</param>
        /// <param name="lockRow">True if you want to write in the row</param>
        private async Task EnterRowAsync(int rowNum, bool forWrite){
            while (!storageBroker.TryEnterReadLock(WAITTIME))
                await Task.Delay(WAITTIME);
            
            if (forWrite){
                if (!lockRowForWrite(rowNum)) {
                    // awaiting lock
                    var tcs = new TaskCompletionSource();
                    LockEventHandler lockHandler = (args) => {
                        if (args.LockNum != rowNum) return;
                        if (lockRowForWrite(rowNum)) tcs.SetResult();
                    };
                    RowReleasedEvent += lockHandler;
                    try {
                        await tcs.Task;
                    } finally {
                        RowReleasedEvent -= lockHandler;
                    }
                }
                // row is write requested; before write need check if there are no readers in the row

                if (!isRowFreeFromReaders(rowNum)){
                    // awaiting free
                    var tcs = new TaskCompletionSource();
                    LockEventHandler lockHandler = (args) => {
                        if (args.LockNum != rowNum) return;
                        if (isRowFreeFromReaders(rowNum)) tcs.SetResult();
                    };
                    RowReleasedEvent += lockHandler;
                    try {
                        await tcs.Task;
                    } finally {
                        RowReleasedEvent -= lockHandler;
                    }
                }
            } else {
                if (!lockRowForRead(rowNum)) {
                    // awaiting lock
                    var tcs = new TaskCompletionSource();
                    LockEventHandler lockHandler = (args) => {
                        if (args.LockNum != rowNum) return;
                        if (lockRowForRead(rowNum)) tcs.SetResult();
                    };
                    LockChangedEvent += lockHandler;
                    try {
                        await tcs.Task;
                    } finally {
                        LockChangedEvent -= lockHandler;
                    }
                }
            }
        }

        /// <summary>
        /// Releases row lock
        /// </summary>
        /// <param name="rowNum">The row number of the row that is being exited.</param>
        /// <param name="fromWrite">true if the row is being exited because a write is being
        /// performed.</param>
        private void ExitRow(int rowNum, bool fromWrite){
            if (fromWrite){
                unlockRowFromWrite(rowNum);
            } else {
                unlockRowFromRead(rowNum);
            }
            storageBroker.ExitReadLock();
        }



        private bool isRowFreeFromReaders(int rowNum){
            rowBroker.EnterReadLock();
            byte locker = (byte)(locks[rowNum] & 127);
            rowBroker.ExitReadLock();
            return (locker==0);
        }

        private bool lockRowForRead(int rowNum){
            rowBroker.EnterUpgradeableReadLock();
            byte locker = locks[rowNum];
            if (locker > 126) { // no free lockers for readers
                rowBroker.ExitUpgradeableReadLock();
                return false;
            }

            rowBroker.EnterWriteLock();
            locks[rowNum]++; // no raising of event LockRowChanded required because no actions are expected on adding new reader;
            rowBroker.ExitWriteLock();
            rowBroker.ExitUpgradeableReadLock();
            return true;
        }

        private bool unlockRowFromRead(int rowNum){
            // always returns true
            rowBroker.EnterUpgradeableReadLock();
            byte locker = locks[rowNum];
            if ((locker & 127) == 0) {
                rowBroker.ExitUpgradeableReadLock();
                throw new Exception("Requested to remove reader from the row when there are 0 readers");
            }

            rowBroker.EnterWriteLock();
            locker = --locks[rowNum];
            rowBroker.ExitWriteLock();
            rowBroker.ExitUpgradeableReadLock();
            if (locker == 0) RowReleasedEvent?.Invoke(new LockEventArgs(rowNum)); // broadcast event "row is free"
            LockChangedEvent?.Invoke(new LockEventArgs(rowNum)); // broadcast event "reader leaved row"
            return true;
        }

        private bool lockRowForWrite(int rowNum){
            rowBroker.EnterUpgradeableReadLock();
            byte locker = locks[rowNum];
            if (locker > 127) { // no free lockers for writers
                rowBroker.ExitUpgradeableReadLock();
                return false;
            }

            rowBroker.EnterWriteLock();
            locks[rowNum] |= 128; // request write lock (set bit#7), no raising of event LockRowChanded required because no actions are expected on adding new writer;
            rowBroker.ExitWriteLock();
            rowBroker.ExitUpgradeableReadLock();
            return true;
        }

        private bool unlockRowFromWrite(int rowNum){
            // always returns true
            rowBroker.EnterUpgradeableReadLock();
            byte locker = locks[rowNum];
            if ((locker & 128) == 0) {
                rowBroker.ExitUpgradeableReadLock();
                throw new Exception("Requested to remove writer from the row when there are 0 writers");
            }

            rowBroker.EnterWriteLock();
            locker = locks[rowNum] &= 127; // ? check if locker become == locks[rowNum]
            rowBroker.ExitWriteLock();
            rowBroker.ExitUpgradeableReadLock();
            if (locker == 0) RowReleasedEvent?.Invoke(new LockEventArgs(rowNum)); // broadcast event "row is free"
            LockChangedEvent?.Invoke(new LockEventArgs(rowNum)); // broadcast event "writer leaved row"
            return true;
        }




        private ItemData find(object item){
            // item not null
            // search item in four rows
            ItemData meta = ComputeHash(item);
            int row = (int)(meta.Hash & HashMask);
            byte sgn = meta.Signature;
            int index;

            if (threshold >= 256) {
                // search in 3 other rows
                // asynchonously start 4 tasks
                Task<int> task = indexOfValueInAsync(item, row, sgn);
                int row2 = (int)(~row & HashMask);
                Task<int> task2 = indexOfValueInAsync(item, row2, sgn);
                int row3 = (int)(((row << 12) | (row >> 12)) & HashMask);
                Task<int> task3 = indexOfValueInAsync(item, row3, sgn);
                int row4 = (int)(~row3 & HashMask);
                Task<int> task4 = indexOfValueInAsync(item, row4, sgn);
                // await results
                index = task.GetAwaiter().GetResult();
                if (index<0) { index = task2.GetAwaiter().GetResult(); meta.Hash = (uint)row2; } // at max BitWidth row equals hash
                if (index<0) { index = task3.GetAwaiter().GetResult(); meta.Hash = (uint)row3; }
                if (index<0) { index = task4.GetAwaiter().GetResult(); meta.Hash = (uint)row4; }
            } else {
                index = indexOfValueIn(item, row, sgn);
            }
            
            return (index<0 ?
                    new ItemData() :
                    meta with {Index = (short)index});
        }

        private async Task<ItemData> findAsync(object item){
            // item not null
            // search item in two rows
            ItemData meta = ComputeHash(item);
            int row = (int)(meta.Hash & HashMask);
            byte sgn = meta.Signature;
            int index;

            if (threshold >= 256) {
                // search in 3 other rows
                // asynchonously start 4 tasks
                Task<int> task = indexOfValueInAsync(item, row, sgn);
                int row2 = (int)(~row & HashMask);
                Task<int> task2 = indexOfValueInAsync(item, row2, sgn);
                int row3 = (int)(((row << 12) | (row >> 12)) & HashMask);
                Task<int> task3 = indexOfValueInAsync(item, row3, sgn);
                int row4 = (int)(~row3 & HashMask);
                Task<int> task4 = indexOfValueInAsync(item, row4, sgn);
                // await results
                index = await task;
                if (index<0) { index = await task2; meta.Hash = (uint)row2; } // at max BitWidth row equals hash
                if (index<0) { index = await task3; meta.Hash = (uint)row3; }
                if (index<0) { index = await task4; meta.Hash = (uint)row4; }
            } else {
                index = await indexOfValueInAsync(item, row, sgn);
            }

            return (index<0 ?
                    new ItemData() :
                    meta with {Index = (short)index});
        }

        // ? ----------------------------------------------------------
        /// <summary>
        /// Returns the index of the first cell in the specified row that contains the specified value, or -1 if the value is not found. Not thread safe!
        /// </summary>
        /// <param name="value">The value to search for.</param>
        /// <param name="row">the row to start searching from</param>
        /// <param name="sgn">0 = equal, 1 = greater than, -1 = less than</param>
        unsafe private int _indexOfValueIn(object value, int row, byte sgn)
        {   
            // value not null

            uint count = freexs[row][0]; if (count==0) return -1;

            // collect indexes with same signatures
            uint index = 0;
            List<uint> indexes = new List<uint>(32); // pre-allocated list
            fixed (byte* position = signatures[row])
            {
                byte* p = position;
                while (count>8){
                    if (*(p+0)==sgn) indexes.Add(index+0);
                    if (*(p+1)==sgn) indexes.Add(index+1);
                    if (*(p+2)==sgn) indexes.Add(index+2);
                    if (*(p+3)==sgn) indexes.Add(index+3);
                    if (*(p+4)==sgn) indexes.Add(index+4);
                    if (*(p+5)==sgn) indexes.Add(index+5);
                    if (*(p+6)==sgn) indexes.Add(index+6);
                    if (*(p+7)==sgn) indexes.Add(index+7);
                    index+=8;
                    count-=8;
                }
                while (count>4){
                    if (*(p+0)==sgn) indexes.Add(index+0);
                    if (*(p+1)==sgn) indexes.Add(index+1);
                    if (*(p+2)==sgn) indexes.Add(index+2);
                    if (*(p+3)==sgn) indexes.Add(index+3);
                    index+=4;
                    count-=4;
                }
                while (count>0){
                    if (*p==sgn) indexes.Add(index);
                    index+=1;
                    count-=1;
                }
            }

            // compare values
            for(int i=0; i<indexes.Count; i++)
                if (value.Equals(values[row][indexes[i]])) return (int)indexes[i];
            
            return -1;
        }
        
        /// <summary>
        /// Returns the index of the first cell in the specified row that contains the specified value, or -1 if the value is not found. Thread safe!
        /// </summary>
        /// <param name="value">The value to search for.</param>
        /// <param name="row">the row to start searching from</param>
        /// <param name="sgn">0 = equal, 1 = greater than, -1 = less than</param>
        private int indexOfValueIn(object value, int row, byte sgn){
            EnterRowAsync(row, false).GetAwaiter().GetResult();
            int i = _indexOfValueIn(value, row, sgn);
            ExitRow(row, false);
            return i;
        }
        
        /// <summary>
        /// Returns the index of the first cell in the specified row that contains the specified value, or -1 if the value is not found. Thread safe!
        /// </summary>
        /// <param name="value">The value to search for.</param>
        /// <param name="row">the row to start searching from</param>
        /// <param name="sgn">0 = equal, 1 = greater than, -1 = less than</param>
        private async Task<int> indexOfValueInAsync(object value, int row, byte sgn){
            await EnterRowAsync(row, false);
            int i = await Task.Run<int>(() => _indexOfValueIn(value, row, sgn));
            ExitRow(row, false);
            return i;
        }

        /// <summary>
        /// Checks if the item is in the storage.
        /// </summary>
        /// <param name="item">The item to search for.</param>
        /// v -------------------------------------------------
        public bool Contains(object? item){
            if (item is null) return false;
            ItemData meta = find(item);
            //if (find(item!).Index >= 0) return true;
            if (meta.Index>=0) return true;
            // + search item in buffer of values to be added (?)
            return false;
        }
        public async Task<bool> ContainsAsync(object? item){
            if (item is null) return false;
            ItemData meta = await findAsync(item);
            //if (find(item!).Index >= 0) return true;
            if (meta.Index>=0) return true;
            // + search item in buffer of values to be added (?)
            return false;
        }

        /// <summary>
        /// Returns the handex of the value in a storage or zero if not found.
        /// </summary>
        /// <param name="item">The item to find.</param>
        /// ? ----------------------------------------------------------------
        public uint Find(object? item){
            if (item is null) return 0;
            ItemData meta = find(item);

            return meta.Index >= 0 ? getHandex(meta) : 0;
        }
    


        /// <summary>
        /// Adds an item to the storage and returns it's handex. If item cannot be saved in storage - returns 0.
        /// </summary>
        /// <param name="item">The object to add to the collection.</param>
        public uint Add(object? item){
            if (item is null) return 0;

            // check if value is already there
            ItemData meta = find(item);
            if (meta.Index >= 0) return getHandex(meta);
    
            // get free index
            int rowNum = (int)(meta.Hash & HashMask);
            byte sgn = meta.Signature;

            EnterRowAsync(rowNum, true).GetAwaiter().GetResult();

            byte index = getFreeIndex(rowNum);
            int rowLength = values[rowNum].Length;

            if (index >= rowLength) {
                // enlarge row
                if (rowLength >= threshold) {
                    // enlarge storage
                    if (bitWidth == HANDEXSHORT_BIT_WIDTH_MAX) {
                        // cannot enlarge storage
                        // inverse row address
                        rowNum = ~rowNum & HashMask;
                        rowLength = values[rowNum].Length;
                        index = getFreeIndex(rowNum);
                        if (index >= rowLength) {
                            // enlarge row
                            if (rowLength >= threshold){
                                // cannot enlarge row
                                // additional row is also full; no place to add item
                                rowBroker.ExitWriteLock();
                                rowBroker.ExitUpgradeableReadLock();
                                storageBroker.ExitUpgradeableReadLock();
                                return 0;
                            }
                        }
                        index = getFreeIndex(rowNum);
                    } else {
                        // enlarge storage; because of our enlargement logic we are sure there will be free place to put item into row after storage enlargement

                    }
                } else {
                    // simple enlargement of row
                    int newRowLength = (int)(Math.Sqrt(rowLength)+1);
                    newRowLength *= newRowLength;
                    setNewRowLength(rowNum, newRowLength);
                }

                rowBroker.ExitUpgradeableReadLock();
                storageBroker.ExitUpgradeableReadLock();
                return 0;
            }

            // adding value
            save(item, rowNum, sgn, index);

            rowBroker.ExitWriteLock();

            rowBroker.ExitUpgradeableReadLock();
            storageBroker.ExitUpgradeableReadLock();

            return rowNum << 8 | index;
        }

        public async Task<uint> AddAsync(object? item){
            return await Task.Run<uint>(() => Add(item));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private uint getHandex(ItemData id){
            return (id.Hash & HashMask) << 8 | (byte)id.Index;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void setNewRowLength(uint rowNum, int newRowLength){
            // rowBroker must be Write Locked

            object[] newRow = new object[newRowLength];
            values[rowNum].CopyTo(newRow,0);
            values[rowNum] = newRow;
            byte[] newSgns = new byte[newRowLength];
            signatures[rowNum].CopyTo(newSgns,0);
            signatures[rowNum] = newSgns;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]    
        private byte getFreeIndex(int row){
            // rowBroker must be in Write Lock status

            List<byte> listIdxs = freexs[row];
            byte index = listIdxs.Last();
            if (listIdxs.Count>1) {
                listIdxs.RemoveAt(index);
                if (listIdxs.Count == 1) listIdxs.TrimExcess();
            }
            return index;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        // public uint Add(object? item){
            
        // }

        // ? ----------------------------------------------
        public object? this[uint id]{
            get {
                if (id==0) return null;
                byte index = (byte)(id & 255);
                uint row = id >> 8;
                // if row is not exist
                if ((row & ~HashMask) > 0) return null;
                return values[row][index];
            }
        }

        // +
        public void Clear(uint size){
            // if count of arrays (rowsCount) > 2^20 this command will shrink array to 2^20 (1M arrays)
            
            // + add code
            
        }


    }

}