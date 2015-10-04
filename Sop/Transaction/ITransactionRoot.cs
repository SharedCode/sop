// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;

namespace Sop.Transaction
{
    public interface ITransactionRoot : ITransactionLogger, IDisposable
    {
        string LogRootFilename { get; }
        void RegisterFailure(TransactionRootFailTypes failType, params object[] parameters);
        void RegisterOpenFile(string filename);
        void RegisterRemoveFile(string filename);
        //Sop.Collections.OnDisk.ObjectServer Server { get; set; }
    }
}
