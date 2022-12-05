// See https://aka.ms/new-console-template for more information
using System.Text;
using DomWiki;
using static DomWiki.Handex;

print("Hello, World!");
Handex storage = new Handex(8);
print(storage.BitWidth + ", " + storage.Count);
List<ulong> addr = new List<ulong>();
foreach (string s in new string[]{"hello", "world", "string"}){
    addr.Add(storage.Add(s));
}
print("count: " + storage.Count);

foreach (ulong a in addr){
    print (a);
};

print("addr: " + addr[0] + ", " + addr[1] + ", " + addr[2]);
print((string)storage.Get(188) + ", " + (int)storage.Get(1));

// uint[] hash(object o) => ComputeHash(o);

void print(object s){
    Console.WriteLine((s is null) ? "null" : s.ToString());
}