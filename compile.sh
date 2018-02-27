rm ./*.class
scalac FpgaJsonParser.scala
rm FpgaJsonParser.h
javah -cp /usr/share/scala/lib/scala-library.jar:. FpgaJsonParser
g++ -fPIC -I$JAVA_HOME/include -I$JAVA_HOME/include/linux -o libFpgaJsonParser.so -shared FpgaJsonParser.cpp

