REQUIREMENTS
------------

Java Development Kit (JDK) version 8 or later

HOW TO BUILD AND INSTALL AUTHORIZATION EXTENSIONS
-------------------------------------------------

1. Compile this authorization sample extension with:

   ./gradlew clean build

2. Copy the jar file `build/libs/authorization.jar` produced at step 1 above
   to the folder `extensions` of your MigratoryData server

   NOTE - If you installed MigratoryData Server using the deb/rpm package,
          then the folder `extensions` is located at `/usr/share/migratorydata`

3. Restart your MigratoryData server
