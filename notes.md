# NOTES

`docker build -t php-dev .`

<!-- In Linux/macOS shells, $(pwd) executes the pwd (print working directory) command and substitutes its output into the command. So if you're in /home/user/project, $(pwd) becomes /home/user/project. -->

`docker run -it -v $(pwd):/var/www/html php-dev`
