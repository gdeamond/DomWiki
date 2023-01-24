// See https://aka.ms/new-console-template for more information
using System.Text;
using DomWiki;
using static DomWiki.Handex;



void print(object s){
    Console.WriteLine((s is null) ? "null" : s.ToString());
}