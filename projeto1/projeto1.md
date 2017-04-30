Resposta para as perguntas do projeto 1 de tabd
===============================================

Aluno: Gilvan Oliveira dos Reis, 21453377
-----------------------------------------


1.
$ head -n 1 wc/part-r-00000
a-breeding	1

2.
$ tail -n 4 wc/part-r-00004 | head -n 1
zeals	1

3.
$ cat wc/part-r-0000* | awk '{if($2 == 1) print $0}' | wc -l
12664

4.
$ head -n 1 wc2/part-r-00000
aaron	96

5.
$ tail -n 4 wc2/part-r-00004 | head -n 1
zeals	1

6.
$ cat wc2/part-r-0000* | awk '{if($2 == 1) print $0}' | wc -l
8027