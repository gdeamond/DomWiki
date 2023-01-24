/**
* @ Author: GDeamond
* @ Create Time: 2022-12-04 18:42:02
* @ Modified time: 2022-12-06 19:31:08
* @ Description: Class Handex organizes hash-table for storage and fast access
*       to CONSTANT IMMUTABLE objects. The principle of this table is similar to 
*       internment of immutable strings in .NET platform. It can be helpful when
*       required to download enormously large amount of like-textual data.
*/

using System.Runtime.CompilerServices;
namespace DomWiki {

    [Serializable]
    internal class Handex {
        /** 
        *       Disadvantages of Generic Dictionary:
        *           - consumes a lot of memory (additional 24 bytes per item! https://habr.com/ru/post/488836/),
        *           - does not provide stable repeatable hashes for its keys (because of depending of hash on item's address in memory),
        *           - cannot keep more than 2M elements.
        *       Disadvantage of Handex:
        *           - objects are immutable (changed object must be re-added and all backlinks to be updated; object's old version still stays there)
        * 
        *       We use strong array of rows (arrays) which consumes next amount of memory:
        *           - less than 10 elements    : 24 - 100 additional bytes per element;
        *           - from 10 to 100 elements  : 12 - 24 additional bytes per element;
        *           - from 100 to 10K elements : 11 additional bytes per element;
        *           - more than 10K elements   : 9 additional bytes per element;
        *   
        *       Since the search unsorted collections is performed linearly, we would not want the rows to be too large. We will split one big array into
        *   small subArrays - rows which will have their address calculated from hash function of the object. Then we just need to perform linear search
        *   in a small collection. Hash function must produce most uniform distribution. We selected xxHash function (https://github.com/uranium62/xxHash).
        *       The address of the particular element consists of 2 parts:
        *           1st - index OF the row (hash result "handex", 8-32 bit),
        *           2nd - index INSIDE the row (which should not be too large, 8-24 bit).
        *       Considering our hash is ideal, the row size should not exceed 2^24 (16M elements), so the most major 8 bits can be used for additional information
        *   about the element - signature: in case if objects are to be changed the old backlink after checking of signature may be rejected (returned null object),
        *   but such mechanism is very not reliable because there is at least 1/256 situaions where requested and stored signatures will match for different objects.
        *   Signatures actually used in Handex for speedup search of elements in the rows as follows: first filtering is hash function, as result we get the array of
        *   different objects. Instead of by sequential search we perform additional filtering by signatures what should increase search speed up to 8 times (because
        *   we check 8 signatures per iteration using unsafe *(long*) ). Signatures are generated with Pearson's hash function.
        *       
        *       What if we need to enlarge the storage?
        *       The occupied memory can be increased in two ways: "horizontally" and "vertically". At the very start all rows are initialized to half of their
        *   threshold size. As soos as at least one row reaches it's initialized size (threshold/2), this particular row grows to threshold size - this is
        *   "horizontal" enlargement.
        *       When the fillness of at least one row reaches the threshold, the "vertical" enlargement happens - new rows added and new rows threshold to be
        *   calculated (as arrayBitWidth^2), additionally we expected the half of elements should move to newly created rows.
        *       Since hashes are stored as full 32-bit values, it's recalculation is not required, but just extend hashMask and run through all rows
        *   for move some elements to new rows - all backlinks for moved elements still points to proper element:
        *           - only that half of elements required movement to other rows which has hash & threhold > 0; element with hash = 11110101 and storage bitWidth = 4
        *               has handex = 0101; after the enlargement of bitWidth the new handex will be 10101 and address become +8, so this element should move to new row;
        *           - but need to place them to the same index of new lists<> which they had in old lists<> — so we provide proper work of backlink;
        *           - when element is moved to non-zero position of just created new list<> we have to track empty fields in that list - this is solved by organizing
        *               separate list of free indexes (what is unpleasant) for memory consumption: in worst case the storage will be increased twice in memory after enlargement.
        *       
        *       Two imprortant things:
        *       1) since Handex does not track backlinks, the only way to optimize Handex (to remove empty or non-used fields) is rebuild it from scratch;
        *       2) it is required to know the type of object you want to get from Handex (because Handex returns "nullable object").
        *   
        *   + upgrade: provide mutability of stored objects
        *       There is possibility to provide mutability for stored objects. Will be need to create an additional reference-layer (another 8 bytes per item). This layer will
        *   function as a broker providing proper reference to the object and refreshing objects state inside the storage. All backlinks will be stored in
        *   reference layer what will consume additional 8 bytes per object - however it is still less than Generic Dictionary. But it will be required
        *   to call "update object" function for update internal reference between "broker" and actual field of the updated object, and also clear old reference.
        *       Disadvantages:
        *           - reduced access speed due to double referencing objects: instead of direct access to array (object=array[row][index]) there will be two steps
        *               (object=broker[address]:array[row][index]) though it can still be enough fast. SpeedTests required.
        *       Solution:
        *           - create super-class implementing or internally using the Handex
        * 
        *   + upgrade: provide thread-safe access to storage
        *       Thread safe access will require locker object per each row. One thread may block one row.
        */

        [NonSerialized] private const uint ARRAY_BIT_WIDTH = 4U; // default size of array (256 elements), and width of handex
        [NonSerialized] private const uint ARRAY_BIT_WIDTH_MIN = 4U; // minimum size of array
        [NonSerialized] private const uint ARRAY_BIT_WIDTH_MAX = 31U; // maximum size of array (2E+12 elements), and width of handex
        // [NonSerialized] private const uint SIGNATURE_MASK = 255U<<<24;
        private uint bitWidth, thresholdRowSize, rowsCount, hashMask, initialRowSize;
        private ulong countElements = 0; // counts all elements
        private ulong countEmpties = 0; // counts free indexes

        private object[]?[]? values; // stores objects except field [0][0] - address of this field means "null"
        private byte[]?[]? signatures; // stores Pearson's hashes
        private object[]? locks; private object mainLock; // stores lockers for rows (one row to be blocked by one thread)
       
        private List<uint>[]? freexs; // stores [0] - length of child array, [1...N] - free indexes (where 0 - not index)

        public ulong Count { get => countElements; }
        public uint BitWidth { get => bitWidth; }

#region StaticFunctions

        private static System.Data.HashFunction.Pearson.IPearson prs = System.Data.HashFunction.Pearson.PearsonFactory.Instance.Create();

        /// <summary>
        /// Splits ulong into two uints. Does not considers Big/Little Endian!
        /// </summary>
        /// <param name="value">The value to split.</param>
        /// <returns>uint[] of two elements</returns>
        /// v -----------------------------------------------------------------------------------------------------------------------------
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        unsafe public static uint[] SplitULong2UInts(ulong value){
            uint* x = (uint*)&value;
            uint* y = x+1;
            
            return new uint[]{*x, *(x+1)};
        }

        /// <summary>
        /// Splits long into two ints. Does not considers Big/Little Endian!
        /// </summary>
        /// <param name="value">The long value to split.</param>
        /// <returns>int[] of two elements</returns>
        /// v -----------------------------------------------------------------------------------------------------------------------------
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        unsafe public static int[] SplitLong2Ints(long value){
            int* x = (int*)&value;
            int* y = x+1;
            
            return new int[]{*x, *(x+1)};
        }

        /// <summary>
        /// Combines two uints into one ulong. Does not considers Big/Little Endian!
        /// </summary>
        /// <param name="int1">The first uint to concatenate.</param>
        /// <param name="int2">The second uint to concatenate.</param>
        /// <returns>ulong</returns>
        /// v -----------------------------------------------------------------------------------------------------------------------------
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        unsafe public static ulong ConcatUInts2ULong(uint int1, uint int2){
            ulong res;
            uint* r = (uint*)&res;
            *r = int1;
            *(r+1) = int2;
            return res;
        }

        /// <summary>
        /// Combines two ints into one long. Does not considers Big/Little Endian!
        /// </summary>z
        /// <param name="int1">The first int to concatenate.</param>
        /// <param name="int2">The second int to concatenate.</param>
        /// <returns>long</returns>
        /// v -----------------------------------------------------------------------------------------------------------------------------
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        unsafe public static long ConcatInts2Long(int int1, int int2){
            long res;
            int* r = (int*)&res;
            *r = int1;
            *(r+1) = int2;
            return res;
        }
        
        /// <summary>
        /// Returns array {xxHash(), PearsonHash()} from object <paramref name="item" />
        /// </summary>
        /// <param name="item"></param>
        /// <returns>array: [0] - 32-bit hash, [1] - 8-bit hash</returns>
        /// v -----------------------------------------------------------------------------------------------------------------------------
        public static uint[] ComputeHash(object? item){ // returns [hash, Pearson's hash]
            uint hs=0, hp=0; // Hash xxhaSh and Hash Pearson's
            if (item is not null) {
                if (item is string) {
                    hs = GetHashFromString((string)item);
                    hp = prs.ComputeHash(System.Text.Encoding.ASCII.GetBytes((string)item)).Hash[0];
                } else {
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
            }
            return new uint[]{hs, hp};
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

        /// <summary>
        /// This class initializes array storage for indexed elements: the higher arrayBitWidth the faster searching of the element
        /// but the more memory will be consumed at start. Maximum arrayBitWidth is 31, minimum 8.
        /// </summary>
        /// <param name="arrayBitWidth">Sets the size of the top array and the width of hash codes</param>
        /// ? -----------------------------------------------------------------------------------------------------------------------------
        public Handex (uint arrayBitWidth = ARRAY_BIT_WIDTH){
            if (arrayBitWidth<8) arrayBitWidth = ARRAY_BIT_WIDTH_MIN;
            if (arrayBitWidth>31) arrayBitWidth = ARRAY_BIT_WIDTH_MAX;

            mainLock = new object();
            initializeWithBitWidth(arrayBitWidth);

            // "add" null-object:
            // ?
            initRow(0);
            save (null, 0, 0, 0);
            freexs![0][0]++; // fillness of the row
        }

        /// ? -----------------------------------------------------------------------------------------------------------------------------
        private void initializeWithBitWidth(uint newBitWidth){
            bitWidth = newBitWidth;
            thresholdRowSize = bitWidth*bitWidth; // length of row at current bitWidth
            rowsCount = 2U<<(int)(bitWidth-1); // = Math.Pow(2, bitWidth)
            hashMask = rowsCount-1;
            initialRowSize = thresholdRowSize>>1; // initial row length

            // create new instance:
            values = new object[rowsCount][];
            signatures = new byte[rowsCount][];
            freexs = new List<uint>[rowsCount];
            locks = new object[rowsCount];
        }

        /// ? -----------------------------------------------------------------------------------------------------------------------------
        private void initRow(uint handex){
            values![handex] = new object[initialRowSize];
            signatures![handex] = new byte[initialRowSize];
            freexs![handex] = new List<uint>(){0};
            locks![handex] = new ReaderWriterLockSlim();
        }

        /// ? -----------------------------------------------------------------------------------------------------------------------------
        private void save(object? item, uint row, uint index, byte signature){
            // ensure row is exist and initialized for proper length, index should be < row length
            #pragma warning disable CS8601
            values![row]![index] = item;
            #pragma warning restore CS8601
            signatures![row]![index] = signature;
        }

        /// <summary>
        /// Searches the storage for and <paramref name="item" /> object and returns its ulong address. If <paramref name="item" /> not found the returns 0.
        /// </summary>
        /// <param name="item">object to find</param>
        /// <returns>ulong address</returns>
        /// + add threadsafe operations -----------------------------------------------------------------------------------------------
        unsafe public ulong Find(object? item){
            if (item is null) return 0;

            uint[] hash = ComputeHash(item);
                uint hs = hash[0]; // Hash
                uint row = hs & hashMask; // Handex
                byte sgn = (byte)hash[1]; // Hash Pearson's -> signature
            
            // + get lock for the row
            object[] valuesRow = values![row]!;

            if (valuesRow is null) return 0; // subArray absent
            uint size = freexs![row][0]; if (size==0) return 0; //subArray is empty

            byte[] signaturesRow = signatures![row]!;
            if (size<16) { // method 1 "dumb"
                // v ---------------------------------------------------------------------------------------------------
                fixed(byte* start = signaturesRow){
                    byte* p = start;
                    uint index = 0;
                    if (row==0) { p+=1; index++; }
                    while (index<size){
                        if (*p==sgn && item.Equals(valuesRow[index]))
                            return ConcatUInts2ULong(row, index);
                        p+=1; index++;
                    }
                }
            } else { // method 2 "filter by signature mask"
                // ? ---------------------------------------------------------------------------------------------------

                // build (ulong)signatureMask = sgn<<56 + sgn<<48 + sgn<<40 + sgn<<32 + sgn<<24 + sgn<<16 + sgn<<8 + sgn
                ulong mask = 0;
                uint* mint = (uint*)&mask + 1;
                byte* m = (byte*)mint;
                *m = sgn; *(m+1) = sgn; *(m+2) = sgn; *(m+3) = sgn;
                *mint = *(uint*)m;

                uint count = (uint)signaturesRow.GetUpperBound(0)+1;
                uint index = 0;
                List<uint> indexes = new List<uint>(32); // pre-allocated list
                // run through signatures[row]: (ulong) slice XOR mask
                fixed(byte* position = signaturesRow){
                    byte* p = position;
                    while (count>8){
                        if ((*(ulong*)p ^ mask) != 0) { // if NZ(slice) {
                            //    subslice = split2ints(slice): if NZ(subslice[0]) get index: if NZ(subslice[1]) get index
                            if ((*(uint*)p ^ *mint)!=0) {
                                if (*p == sgn) indexes.Add(index);
                                if (*(p+1) == sgn) indexes.Add(index+1);
                                if (*(p+2) == sgn) indexes.Add(index+2);
                                if (*(p+3) == sgn) indexes.Add(index+3);
                            }
                            if ((*(uint*)(p+4) ^ *mint)!=0) {
                                if (*(p+4) == sgn) indexes.Add(index+4);
                                if (*(p+5) == sgn) indexes.Add(index+5);
                                if (*(p+6) == sgn) indexes.Add(index+6);
                                if (*(p+7) == sgn) indexes.Add(index+7);
                            }
                        }
                        p += 8;
                        count -= 8;
                    }
                    // work for last <8 elements
                    for (; count>0; p++, index++, count--)
                        if (*p == sgn) indexes.Add(index);
                }
                // run through found indexes[] in "dumb" way
                for (int i=0; i<indexes.Count; i++)
                    if (item.Equals(valuesRow[indexes[i]]))
                        return ConcatUInts2ULong(row, indexes[i]);
            }

            return 0;
        }

        // ? ----------------------------------------------
        public object? this[ulong id]{
            get {
                if (id==0) return null;
                uint[] uints = SplitULong2UInts(id);
                uint row = uints[0];
                uint index = uints[1];
                //if (values[row] is null) return null;
                //if (index>=freexs[row][0]) return null;
                return values![row]?[index] ?? null;
            }
        }

        /// v -----------------------------------------------------------------------------------------------------------------------------
        public bool Contains(object item){
            return Find(item)>0;
        }

        // +
        public void Clear(uint size){
            // if count of arrays (rowsCount) > 2^20 this command will shrink array to 2^20 (1M arrays)
            
            // + add code
            
        }


        // + make threadsafe
        public ulong Add(object? item){
            if (item is null) return 0;

            uint[] hash = ComputeHash(item); // HandeX, Hash Pearson's
                uint hs = hash[0]; // Hash
                uint row = hs & hashMask; // Handex
                byte sgn = (byte)hash[1]; // Hash Pearson's -> signature
            
            if (values![row] is null) initRow(row); // subArray absent
            
            uint size = freexs![row][0];

            // check if there is no same object
            if (size>0) {
                byte[] sgns = signatures![row]!;
                unsafe {
                    fixed(byte* start = sgns){
                        byte* p = start;
                        uint index = 0;
                        if (row==0) { p+=1; index++; }
                        while (index<size){
                            if (*p==sgn && item.Equals(values[row]![index]))
                                return ConcatUInts2ULong(row, index);
                            p+=1; index++;
                        }
                    }
                }
            }

            // get next index
            List<uint> freeIndxs = freexs[row];
            uint newIndex = freeIndxs[0]; // count of elements in row
            int lastPos = freeIndxs.Count-1;
            if (lastPos>0) {
                newIndex = freeIndxs.ElementAt(lastPos);
                freeIndxs.RemoveAt(lastPos);
            }

            // add new element
            save(item, row, newIndex, sgn); // sets item and signature
            countElements++;
            freeIndxs[0]++;

            
            return ConcatUInts2ULong(row, newIndex);
        }


        // check if there is required remap/enlarging of array
        // + provide parallel tasks for storage enlargement job
        private void checkForEnlargement(uint row) {
            int upperBound = values![row]!.GetUpperBound(0);
            uint lastIndex = freexs![row][0];
            if (lastIndex >= upperBound){
                if (lastIndex >= (thresholdRowSize-1)){
                    // row length achieved threshold, consider vertical enlargement
                    // + start Task for reorganize storage

                } else {
                    
                }
            }
        }

        private void setRowLength(uint row, uint length){
            // lock(values![row]!){
                object[] newRow = new object[length];
                Array.Copy(values![row]!, newRow, length);
                byte[] newSgns = new byte[length];
                FastMath.fastCopy(signatures![row]!, newSgns, (int)length);
                values[row] = newRow;
                signatures[row] = newSgns;
            
        }

    }

}