
var shell = require('shelljs');
shell.cd();


shell.echo(shell.pwd());

if (shell.exec('python3').code !== 0) {
    shell.echo('Error: Git commit failed');
    shell.exit(1);
}
// console.log(shell.ls());
// // console.log(x);
// // shell.echo(x);

