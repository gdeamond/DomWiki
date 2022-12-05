
namespace DomWiki {

    [Serializable]
    internal class Handex {
        /* 
                Disadvantages of Generic Dictionary: - consumes a lot of memory (24+ bytes per item), - does not provide stable repeatable hashes for its keys,
            - cannot keep more than 11M elements.
                We use strong array of rows (arrays). This collection consumes a lot of memory at start (starting size), but when quantity of elements
            increased, the part of starting size in a whole size of the storage become smaller:
                    - Size 2^20 - is 1M empty elements at start, 8 MB of memory for references to Lists<string> on x64 systems.
                    - Each string (as reference type) will add 8 bytes per element to infrastructure. In total the consumed memory for 1M elements
                        will be 16+ bytes per item. But 8M elements consumes 9+ bytes per item what is acceptable.
            
                Since the search in sequential collections is performed linearly, we would not want the lists to be too large.
            So there should be proper hash function with the most uniform distribution. We selected xxHash function (https://github.com/uranium62/xxHash).
                Address of element consists of 2 values: 1st - index of the row (handex, 8-32 bit), 2nd - index in the row (which should not be too large).
            It is very unbelievable to have row size > 2^24 (16M elements), so the most major 8 bits we will use for store additional information about the
            element: we will use it for signature - this can speedup search of elements by its values filtering list by signature. Signature is generated
            as Pearson's hash. In case if the row size become > arrayBitWidth^2 [in other word - if fillness of Handex is going to "wide rectangle"],
            the storage can be enlarged.
                Since hashes are stored as full 32-bit values, it's recalculation is not required, but just extend hashMask and run through all rows
            for move some elements to new rows. Of course we should be sure that all backlinks for moved elements still points to proper element.
            But this is solved by next logic:
                - only half of elements will require movement to other rows; element with hash = 11110101 and storage bitWidth = 4 has handex = 0101; after the
                    enlargement of bitWidth the new handex will be 10101 and address become +8, and this element should move to new row;
                - but need to place them to the same index of new lists<> which they had in old lists<> — so we dont have to change backlinks at all;
                - moving element to non-zero position of new list<> we have to track which fields are empty in that list - this is solved by organizing separate
                    list of free indexes (what is unpleasant for memory consumption: in worth case the storage size increases twice in memory).
                
                The occupied memory can be increased in two ways: horizontally and vertically. At the very start all rows are initialized at half of their
            threshold size. As soos as at least one row reaches it's initialized size (threshold/2), this particular row grows to threshold size - this is
            horizontal enlargement. Next time when the fillness of at least one row reaches threshold, the vertical enlargement happens - new rows added and
            new row threshold will be calculated, additionally we expected that half of the elements from each row will move to newly created rows, and after
            what we expect that free indexes will appear in old rows.
            
                Two imprortant thing:
                1) since Handex does not track backlinks, the only way to optimize Handex (to remove empty or non-used fields)
                    is rebuild it from scratch;
                2) it is required to know the type of object you want to get from Handex.
            
                How elements with same hash are stored:
            item | reference[][]          | address for reference
            : 1  | : [handex1][index1]    | = handex[32 bit] + signature1[8 bit] + index1[24 bit];
            : 2  | : [handex1][index2]    | = handex[32 bit] + signature2[8 bit] + index2[24 bit];
        */

#region #region StaticFunctions

        private static System.Data.HashFunction.Pearson.IPearson prs = System.Data.HashFunction.Pearson.PearsonFactory.Instance.Create();

        /// <summary>
        /// Splits ulong into two uints. Does not considers Big/Little Endian!
        /// </summary>
        /// <param name="value">The value to split.</param>
        /// <returns>uint[] of two elements</returns>
        /// v -----------------------------------------------------------------------------------------------------------------------------
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
        public static uint GetHashFromString(string value){
            return (Standart.Hash.xxHash.xxHash32.ComputeHash(value));
        }

        /// <summary>
        /// Returns xxHash() from byte stream <paramref name="strm" />
        /// </summary>
        /// <param name="strm">stream</param>
        /// <returns>uint hash value</returns>
        /// v -----------------------------------------------------------------------------------------------------------------------------
        public static uint GetHashFromStream(Stream strm){
            return (Standart.Hash.xxHash.xxHash32.ComputeHash(strm));
        }

#endregion 

        [NonSerialized] private const uint ARRAY_BIT_WIDTH = 10U; // starting size of array (1K elements), and width of handex (index in hash array)
        // [NonSerialized] private const uint SIGNATURE_MASK = 255U<<24;
        private uint bitWidth, threshold, rowsCount, hashMask, subArraySize;
        private ulong countElements = 0; // counts all elements
        private ulong countEmpties = 0; // counts free indexes

        private object[][] values; // stores objects except field [0][0] - address of this field means "null"
        private byte[][] signatures; // stores Pearson's hashes of objects
        private List<uint>[] freexs; // stores [0] - length of child array, [1...N] - free indexes (where 0 - not index)
        

        public ulong Count { get => countElements; }
        public uint BitWidth { get => bitWidth; }

        /// <summary>
        /// This class initializes array storage for indexed elements: the higher arrayBitWidth the faster searching of the element
        /// but the more memory will be consumed at start. Maximum arrayBitWidth is 31, minimum 8.
        /// </summary>
        /// <param name="arrayBitWidth">Sets the size of the top array and the width of hash codes</param>
        /// ? -----------------------------------------------------------------------------------------------------------------------------
        public Handex (uint arrayBitWidth = ARRAY_BIT_WIDTH){
            if (arrayBitWidth<8) arrayBitWidth=8;
            if (arrayBitWidth>31) arrayBitWidth = 31;

            setBitWidth(arrayBitWidth);

            // create new instance
            values = new object[rowsCount][];
            signatures = new byte[rowsCount][];
            freexs = new List<uint>[rowsCount];

            // "add" null-object:
            // ?
            addRow(0);
            save (null, 0, 0, 0);
            freexs[0][0]++; // fillness of the row
        }

        /// <summary>
        /// Searches the storage for and <paramref name="item" /> object and returns its ulong address. If <paramref name="item" /> not found the returns 0.
        /// </summary>
        /// <param name="item">object to find</param>
        /// <returns>ulong address</returns>
        /// ? -----------------------------------------------------------------------------------------------------------------------------
        public ulong Find(object? item){
            if (item is null) return 0;

            uint[] hash = ComputeHash(item);
                uint hs = hash[0]; // Hash
                uint row = hs & hashMask; // Handex
                byte sgn = (byte)hash[1]; // Hash Pearson's -> signature
            
            if (values[row] is null) return 0; // subArray absent
            
            uint size = freexs[row][0]; if (size==0) return 0;
            byte[] sgns = signatures[row];
            unsafe {
                fixed(byte* start = &sgns[0]){
                    byte* p = start;
                    uint index = 0;
                    if (row==0) { p+=1; index++; }
                    while (index<size){
                        if (*p==sgn && item.Equals(values[row][index]))
                            return ConcatUInts2ULong(row, index);
                        p+=1; index++;
                    }
                }
            }
            return 0;
        }

        /// v -----------------------------------------------------------------------------------------------------------------------------
        public bool Contains(object item){
            return Find(item)>0;
        }

        /// ? -----------------------------------------------------------------------------------------------------------------------------
        public object? Get(ulong address){
            if (address==0) return null;
            uint[] uints = SplitULong2UInts(address);
            uint row = uints[0];
            uint index = uints[1];
            //if (values[row] is null) return null;
            //if (index>=freexs[row][0]) return null;
            return values[row]?[index] ?? null;
        }

        // +
        public void Clear(uint size){
            // if count of arrays (rowsCount) > 2^20 this command will shrink array to 2^20 (1M arrays)
            
            // + add code
            
        }


        // +
        public ulong Add(object? item){
            if (item is null) return 0;

            uint[] hash = ComputeHash(item); // HandeX, Hash Pearson's
                uint hs = hash[0]; // Hash
                uint row = hs & hashMask; // Handex
                byte sgn = (byte)hash[1]; // Hash Pearson's -> signature
            
            if (values[row] is null) addRow(row); // subArray absent
            
            uint size = freexs[row][0];

            // check if there is no same object
            if (size>0) {
                byte[] sgns = signatures[row];
                unsafe {
                    fixed(byte* start = &sgns[0]){
                        byte* p = start;
                        uint index = 0;
                        if (row==0) { p+=1; index++; }
                        while (index<size){
                            if (*p==sgn && item.Equals(values[row][index]))
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
            int upperBound = values[row].GetUpperBound(0);
            uint lastIndex = freexs[row][0];
            if (lastIndex >= upperBound){
                if (lastIndex >= (threshold-1)){
                    // row length achieved threshold, consider vertical enlargement
                    // + start Task for reorganize storage

                } else {
                    
                }
            }
        }

        private void setRowLength(uint row, uint length){
            lock(values[row]){
                object[] newRow = new object[length];
                Array.Copy(values[row], newRow, length);
                byte[] newSgns = new byte[length];
                FastMath.fastCopy(signatures[row], newSgns, (int)length);
                values[row] = newRow;
                signatures[row] = newSgns;
            }
        }

        /// ? -----------------------------------------------------------------------------------------------------------------------------
        private void save(object? item, uint row, uint index, byte signature){
            // ensure row is exist and initialized for proper length, index should be < row length
            #pragma warning disable CS8601
            values[row][index] = item;
            #pragma warning restore CS8601
            signatures[row][index] = signature;
        }

        /// ? -----------------------------------------------------------------------------------------------------------------------------
        private void addRow(uint handex){
            values[handex] = new object[subArraySize];
            signatures[handex] = new byte[subArraySize];
            freexs[handex] = new List<uint>(){0};
        }

        /// ? -----------------------------------------------------------------------------------------------------------------------------
        private void setBitWidth(uint newBitWidth){
            bitWidth = newBitWidth;
            threshold = bitWidth*bitWidth;
            rowsCount = 2U<<(int)(bitWidth-1);
            hashMask = rowsCount-1;
            subArraySize = threshold>>1;
        }

    }

}