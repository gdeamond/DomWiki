namespace DomWiki {
    public class DColumn {
        private List<ulong>? values;
        private List<ulong>? trash; // keeps indexes of removed values


        public enum DataType : byte {
            Int = 0, //
            Float,  //
            Date,   //
            Text,   //
            Object, //
            Column,  // DColumn
            Row, // DColumn
            Table, // DTable
        }

        public DColumn(DataType type){
            //if type<DataType.Text 
        }
    }
}