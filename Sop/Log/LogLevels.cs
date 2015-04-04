using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sop.Log
{
    /// <summary>
    /// Different Log levels supported
    /// </summary>
    public enum LogLevels
    {
        /// <summary>
        /// Logs verbose output.
        /// </summary>
        Verbose = 1,
        /// <summary>
        /// Logs basic information.NOTE: conside Info
        /// </summary>
        Information = 2,
        /// <summary>
        /// Logs a warning.
        /// </summary>
        Warning = 3,
        /// <summary>
        /// Logs an error.
        /// </summary>
        Error = 4,
        /// <summary>
        /// Logs a fatal incident.
        /// </summary>
        Fatal = 5
    }
}
