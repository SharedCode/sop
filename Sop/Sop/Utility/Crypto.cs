// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System.Text;
using System;
using System.Security.Cryptography;
using System.IO;

namespace Sop.Utility
{
    /// <summary>
    /// SOP's internal Cryptography class.
    /// </summary>
    internal class Crypto
    {
        private static readonly byte[] _salt = Encoding.ASCII.GetBytes("o6806642kbM7c5");

        /// <summary> 
        /// Encrypt the given string using AES.  The string can be decrypted using  
        /// DecryptString().  The sharedSecret parameters must match. 
        /// </summary> 
        /// <param name="plainText">The text to encrypt.</param> 
        /// <param name="sharedSecret">A password used to generate a key for encryption.</param> 
        public static string EncryptString(string plainText, string sharedSecret)
        {
            if (string.IsNullOrEmpty(plainText))
                throw new ArgumentNullException("plainText");
            if (string.IsNullOrEmpty(sharedSecret))
                throw new ArgumentNullException("sharedSecret");

            string outStr = null; // Encrypted string to return 
            RijndaelManaged aesAlg = null; // RijndaelManaged object used to encrypt the data. 

            try
            {
                // generate the key from the shared secret and the salt 
                var key = new Rfc2898DeriveBytes(sharedSecret, _salt);

                // Create a RijndaelManaged object 
                // with the specified key and IV. 
                aesAlg = new RijndaelManaged();
                aesAlg.Key = key.GetBytes(aesAlg.KeySize/8);
                aesAlg.IV = key.GetBytes(aesAlg.BlockSize/8);

                // Create a decrytor to perform the stream transform. 
                using (ICryptoTransform encryptor = aesAlg.CreateEncryptor(aesAlg.Key, aesAlg.IV))
                {
                    // Create the streams used for encryption. 
                    using (MemoryStream msEncrypt = new MemoryStream())
                    {
                        using (CryptoStream csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write))
                        {
                            using (StreamWriter swEncrypt = new StreamWriter(csEncrypt))
                            {
                                //Write all data to the stream. 
                                swEncrypt.Write(plainText);
                            }
                        }
                        outStr = Convert.ToBase64String(msEncrypt.ToArray());
                    }
                }
            }
            finally
            {
                // Clear the RijndaelManaged object. 
                if (aesAlg != null)
                    aesAlg.Clear();
            }

            // Return the encrypted bytes from the memory stream. 
            return outStr;
        }

        /// <summary> 
        /// Decrypt the given string.  Assumes the string was encrypted using  
        /// EncryptString(), using an identical sharedSecret. 
        /// </summary> 
        /// <param name="cipherText">The text to decrypt.</param> 
        /// <param name="sharedSecret">A password used to generate a key for decryption.</param> 
        public static string DecryptString(string cipherText, string sharedSecret)
        {
            if (string.IsNullOrEmpty(cipherText))
                throw new ArgumentNullException("cipherText");
            if (string.IsNullOrEmpty(sharedSecret))
                throw new ArgumentNullException("sharedSecret");

            // Declare the RijndaelManaged object 
            // used to decrypt the data. 
            RijndaelManaged aesAlg = null;

            // Declare the string used to hold 
            // the decrypted text. 
            string plaintext = null;

            try
            {
                // generate the key from the shared secret and the salt 
                var key = new Rfc2898DeriveBytes(sharedSecret, _salt);

                // Create a RijndaelManaged object 
                // with the specified key and IV. 
                aesAlg = new RijndaelManaged();
                aesAlg.Key = key.GetBytes(aesAlg.KeySize/8);
                aesAlg.IV = key.GetBytes(aesAlg.BlockSize/8);

                // Create a decrytor to perform the stream transform. 
                using (ICryptoTransform decryptor = aesAlg.CreateDecryptor(aesAlg.Key, aesAlg.IV))
                {
                    // Create the streams used for decryption.                 
                    byte[] bytes = Convert.FromBase64String(cipherText);
                    using (MemoryStream msDecrypt = new MemoryStream(bytes))
                    {
                        using (CryptoStream csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read))
                        {
                            using (StreamReader srDecrypt = new StreamReader(csDecrypt))

                                // Read the decrypted bytes from the decrypting stream 
                                // and place them in a string. 
                                plaintext = srDecrypt.ReadToEnd();
                        }
                    }
                }
            }
            finally
            {
                // Clear the RijndaelManaged object. 
                if (aesAlg != null)
                    aesAlg.Clear();
            }

            return plaintext;
        }
    }
}