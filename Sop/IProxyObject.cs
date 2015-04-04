// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;

namespace Sop
{
    /// <summary>
    /// SOP's virtual Proxy Object is a wrapper class for containing actual object
    /// reference to any object proxied by the instance.
    /// </summary>
    public interface IProxyObject : IProxyObject<object>
    {
    }

    /// <summary>
    /// Generic version of the SOP virtual Proxy Object wrapper class.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IProxyObject<T>
    {
        /// <summary>
        /// Contains reference to the real object this proxy is a wrapper of.
        /// NOTE: this is for SOP internal framework use only.
        /// </summary>
        T RealObject { get; set; }
    }
}