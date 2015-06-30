%%% report.mp -- do not edit, generated automatically by report.tex
verbatimtex
\documentclass [11pt]{article}
\begin {document}
etex;
input metauml; \end {empcmds} \begin {emp}[[](100,100) DURefreshThread]
        Class.A("Point")
               ("+x: int",
                "+y: int") ();
        drawObjects(A, B);
    \end{XeClassDiagram}

    \begin{XeMethod}{\XePublic}{DU}{DU}
        Keeps track of disk usage.
    \end{XeMethod}

    \begin{XeMethod}{\XePrivate}{DU}{DU}
        Keeps track of disk usage.
    \end{XeMethod}

    \begin{XeMethod}{\XeProtected}{void}{decDfsUsed}
        Decrease how much disk space we use.
    \end{XeMethod}

    \begin{XeMethod}{\XePublic}{void}{incDfsUsed}
        Increase how much disk space we use.
    \end{XeMethod}

    \begin{XeMethod}{\XePublic}{long}{getUsed}

    \end{XeMethod}

    \begin{XeMethod}{\XePublic}{String}{getDirPath}

    \end{XeMethod}

    \begin{XeMethod}{\XePublic}{void}{start}
        Start the disk usage checking thread.
    \end{XeMethod}

    \begin{XeMethod}{\XePublic}{void}{shutdown}
        Shut down the refreshing thread.
    \end{XeMethod}

\end{XeClass}
