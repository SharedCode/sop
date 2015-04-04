namespace Sop.Samples
{
	partial class BayWindBrowser
	{
		/// <summary>
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.IContainer components = null;

		/// <summary>
		/// Clean up any resources being used.
		/// </summary>
		/// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
		protected override void Dispose(bool disposing)
		{
			if (disposing && (components != null))
			{
				components.Dispose();
			}
			base.Dispose(disposing);
		}

		#region Windows Form Designer generated code

		/// <summary>
		/// Required method for Designer support - do not modify
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
			this.components = new System.ComponentModel.Container();
			this.buttonInitDB = new System.Windows.Forms.Button();
			this.dataGridViewPeople = new System.Windows.Forms.DataGridView();
			this.label1 = new System.Windows.Forms.Label();
			this.buttonAdd = new System.Windows.Forms.Button();
			this.label3 = new System.Windows.Forms.Label();
			this.textBoxFirstName = new System.Windows.Forms.TextBox();
			this.textBoxLastName = new System.Windows.Forms.TextBox();
			this.label4 = new System.Windows.Forms.Label();
			this.textBoxPhone = new System.Windows.Forms.TextBox();
			this.label5 = new System.Windows.Forms.Label();
			this.textBoxStreet = new System.Windows.Forms.TextBox();
			this.label6 = new System.Windows.Forms.Label();
			this.textBoxCity = new System.Windows.Forms.TextBox();
			this.label7 = new System.Windows.Forms.Label();
			this.textBoxState = new System.Windows.Forms.TextBox();
			this.label8 = new System.Windows.Forms.Label();
			this.textBoxCountry = new System.Windows.Forms.TextBox();
			this.label9 = new System.Windows.Forms.Label();
			this.buttonCommit = new System.Windows.Forms.Button();
			this.buttonDelete = new System.Windows.Forms.Button();
			this.buttonSearch = new System.Windows.Forms.Button();
			this.buttonUpdate = new System.Windows.Forms.Button();
			this.buttonQuit = new System.Windows.Forms.Button();
			this.label2 = new System.Windows.Forms.Label();
			this.textBoxBayWindDBFilename = new System.Windows.Forms.TextBox();
			this.buttonFirstPage = new System.Windows.Forms.Button();
			this.buttonMovePreviousPage = new System.Windows.Forms.Button();
			this.buttonMoveNextPage = new System.Windows.Forms.Button();
			this.buttonMoveLastPage = new System.Windows.Forms.Button();
			this.textBoxCount = new System.Windows.Forms.TextBox();
			this.label10 = new System.Windows.Forms.Label();
			this.textBoxZip = new System.Windows.Forms.TextBox();
			this.label11 = new System.Windows.Forms.Label();
			this.toolTip1 = new System.Windows.Forms.ToolTip(this.components);
			((System.ComponentModel.ISupportInitialize)(this.dataGridViewPeople)).BeginInit();
			this.SuspendLayout();
			// 
			// buttonInitDB
			// 
			this.buttonInitDB.Location = new System.Drawing.Point(437, 449);
			this.buttonInitDB.Name = "buttonInitDB";
			this.buttonInitDB.Size = new System.Drawing.Size(148, 23);
			this.buttonInitDB.TabIndex = 0;
			this.buttonInitDB.Text = "ReCreate Bay Wind DB";
			this.toolTip1.SetToolTip(this.buttonInitDB, "Re-create the Bay Wind database");
			this.buttonInitDB.UseVisualStyleBackColor = true;
			this.buttonInitDB.Click += new System.EventHandler(this.buttonInitDB_Click);
			// 
			// dataGridViewPeople
			// 
			this.dataGridViewPeople.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
			this.dataGridViewPeople.Location = new System.Drawing.Point(29, 170);
			this.dataGridViewPeople.MultiSelect = false;
			this.dataGridViewPeople.Name = "dataGridViewPeople";
			this.dataGridViewPeople.ReadOnly = true;
			this.dataGridViewPeople.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.FullRowSelect;
			this.dataGridViewPeople.Size = new System.Drawing.Size(360, 267);
			this.dataGridViewPeople.TabIndex = 1;
			this.dataGridViewPeople.SelectionChanged += new System.EventHandler(this.dataGridViewPeople_SelectionChanged);
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Location = new System.Drawing.Point(26, 145);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(43, 13);
			this.label1.TabIndex = 2;
			this.label1.Text = "People:";
			// 
			// buttonAdd
			// 
			this.buttonAdd.Location = new System.Drawing.Point(437, 203);
			this.buttonAdd.Name = "buttonAdd";
			this.buttonAdd.Size = new System.Drawing.Size(59, 23);
			this.buttonAdd.TabIndex = 5;
			this.buttonAdd.Text = "Add";
			this.toolTip1.SetToolTip(this.buttonAdd, "Add Item");
			this.buttonAdd.UseVisualStyleBackColor = true;
			this.buttonAdd.Click += new System.EventHandler(this.buttonAdd_Click);
			// 
			// label3
			// 
			this.label3.AutoSize = true;
			this.label3.Location = new System.Drawing.Point(27, 41);
			this.label3.Name = "label3";
			this.label3.Size = new System.Drawing.Size(60, 13);
			this.label3.TabIndex = 6;
			this.label3.Text = "First Name:";
			// 
			// textBoxFirstName
			// 
			this.textBoxFirstName.Location = new System.Drawing.Point(94, 38);
			this.textBoxFirstName.Name = "textBoxFirstName";
			this.textBoxFirstName.Size = new System.Drawing.Size(131, 20);
			this.textBoxFirstName.TabIndex = 7;
			// 
			// textBoxLastName
			// 
			this.textBoxLastName.Location = new System.Drawing.Point(94, 64);
			this.textBoxLastName.Name = "textBoxLastName";
			this.textBoxLastName.Size = new System.Drawing.Size(131, 20);
			this.textBoxLastName.TabIndex = 9;
			// 
			// label4
			// 
			this.label4.AutoSize = true;
			this.label4.Location = new System.Drawing.Point(27, 67);
			this.label4.Name = "label4";
			this.label4.Size = new System.Drawing.Size(61, 13);
			this.label4.TabIndex = 8;
			this.label4.Text = "Last Name:";
			// 
			// textBoxPhone
			// 
			this.textBoxPhone.Location = new System.Drawing.Point(94, 89);
			this.textBoxPhone.Name = "textBoxPhone";
			this.textBoxPhone.Size = new System.Drawing.Size(131, 20);
			this.textBoxPhone.TabIndex = 11;
			// 
			// label5
			// 
			this.label5.AutoSize = true;
			this.label5.Location = new System.Drawing.Point(37, 92);
			this.label5.Name = "label5";
			this.label5.Size = new System.Drawing.Size(51, 13);
			this.label5.TabIndex = 10;
			this.label5.Text = "Phone #:";
			// 
			// textBoxStreet
			// 
			this.textBoxStreet.Location = new System.Drawing.Point(340, 35);
			this.textBoxStreet.Name = "textBoxStreet";
			this.textBoxStreet.Size = new System.Drawing.Size(245, 20);
			this.textBoxStreet.TabIndex = 13;
			// 
			// label6
			// 
			this.label6.AutoSize = true;
			this.label6.Location = new System.Drawing.Point(298, 38);
			this.label6.Name = "label6";
			this.label6.Size = new System.Drawing.Size(38, 13);
			this.label6.TabIndex = 12;
			this.label6.Text = "Street:";
			// 
			// textBoxCity
			// 
			this.textBoxCity.Location = new System.Drawing.Point(340, 61);
			this.textBoxCity.Name = "textBoxCity";
			this.textBoxCity.Size = new System.Drawing.Size(245, 20);
			this.textBoxCity.TabIndex = 15;
			// 
			// label7
			// 
			this.label7.AutoSize = true;
			this.label7.Location = new System.Drawing.Point(309, 64);
			this.label7.Name = "label7";
			this.label7.Size = new System.Drawing.Size(27, 13);
			this.label7.TabIndex = 14;
			this.label7.Text = "City:";
			// 
			// textBoxState
			// 
			this.textBoxState.Location = new System.Drawing.Point(340, 87);
			this.textBoxState.Name = "textBoxState";
			this.textBoxState.Size = new System.Drawing.Size(85, 20);
			this.textBoxState.TabIndex = 17;
			// 
			// label8
			// 
			this.label8.AutoSize = true;
			this.label8.Location = new System.Drawing.Point(301, 90);
			this.label8.Name = "label8";
			this.label8.Size = new System.Drawing.Size(35, 13);
			this.label8.TabIndex = 16;
			this.label8.Text = "State:";
			// 
			// textBoxCountry
			// 
			this.textBoxCountry.Location = new System.Drawing.Point(340, 116);
			this.textBoxCountry.Name = "textBoxCountry";
			this.textBoxCountry.Size = new System.Drawing.Size(245, 20);
			this.textBoxCountry.TabIndex = 19;
			// 
			// label9
			// 
			this.label9.AutoSize = true;
			this.label9.Location = new System.Drawing.Point(290, 119);
			this.label9.Name = "label9";
			this.label9.Size = new System.Drawing.Size(46, 13);
			this.label9.TabIndex = 18;
			this.label9.Text = "Country:";
			// 
			// buttonCommit
			// 
			this.buttonCommit.Location = new System.Drawing.Point(437, 336);
			this.buttonCommit.Name = "buttonCommit";
			this.buttonCommit.Size = new System.Drawing.Size(59, 23);
			this.buttonCommit.TabIndex = 20;
			this.buttonCommit.Text = "Commit";
			this.toolTip1.SetToolTip(this.buttonCommit, "Commit Changes");
			this.buttonCommit.UseVisualStyleBackColor = true;
			this.buttonCommit.Click += new System.EventHandler(this.buttonCommit_Click);
			// 
			// buttonDelete
			// 
			this.buttonDelete.Location = new System.Drawing.Point(437, 232);
			this.buttonDelete.Name = "buttonDelete";
			this.buttonDelete.Size = new System.Drawing.Size(59, 23);
			this.buttonDelete.TabIndex = 22;
			this.buttonDelete.Text = "Delete";
			this.toolTip1.SetToolTip(this.buttonDelete, "Delete selected Item");
			this.buttonDelete.UseVisualStyleBackColor = true;
			this.buttonDelete.Click += new System.EventHandler(this.buttonDelete_Click);
			// 
			// buttonSearch
			// 
			this.buttonSearch.Location = new System.Drawing.Point(226, 37);
			this.buttonSearch.Name = "buttonSearch";
			this.buttonSearch.Size = new System.Drawing.Size(49, 23);
			this.buttonSearch.TabIndex = 23;
			this.buttonSearch.Text = "Search";
			this.toolTip1.SetToolTip(this.buttonSearch, "Search Person with given First Name");
			this.buttonSearch.UseVisualStyleBackColor = true;
			this.buttonSearch.Click += new System.EventHandler(this.buttonSearch_Click);
			// 
			// buttonUpdate
			// 
			this.buttonUpdate.Location = new System.Drawing.Point(437, 261);
			this.buttonUpdate.Name = "buttonUpdate";
			this.buttonUpdate.Size = new System.Drawing.Size(59, 23);
			this.buttonUpdate.TabIndex = 24;
			this.buttonUpdate.Text = "Update";
			this.toolTip1.SetToolTip(this.buttonUpdate, "Update Item");
			this.buttonUpdate.UseVisualStyleBackColor = true;
			this.buttonUpdate.Click += new System.EventHandler(this.buttonUpdate_Click);
			// 
			// buttonQuit
			// 
			this.buttonQuit.Location = new System.Drawing.Point(437, 365);
			this.buttonQuit.Name = "buttonQuit";
			this.buttonQuit.Size = new System.Drawing.Size(59, 23);
			this.buttonQuit.TabIndex = 25;
			this.buttonQuit.Text = "Quit";
			this.toolTip1.SetToolTip(this.buttonQuit, "Close window and Quit");
			this.buttonQuit.UseVisualStyleBackColor = true;
			this.buttonQuit.Click += new System.EventHandler(this.buttonQuit_Click);
			// 
			// label2
			// 
			this.label2.AutoSize = true;
			this.label2.Location = new System.Drawing.Point(26, 454);
			this.label2.Name = "label2";
			this.label2.Size = new System.Drawing.Size(116, 13);
			this.label2.TabIndex = 26;
			this.label2.Text = "BayWind DB Filename:";
			// 
			// textBoxBayWindDBFilename
			// 
			this.textBoxBayWindDBFilename.Location = new System.Drawing.Point(153, 451);
			this.textBoxBayWindDBFilename.Name = "textBoxBayWindDBFilename";
			this.textBoxBayWindDBFilename.ReadOnly = true;
			this.textBoxBayWindDBFilename.Size = new System.Drawing.Size(209, 20);
			this.textBoxBayWindDBFilename.TabIndex = 27;
			// 
			// buttonFirstPage
			// 
			this.buttonFirstPage.Location = new System.Drawing.Point(89, 140);
			this.buttonFirstPage.Name = "buttonFirstPage";
			this.buttonFirstPage.Size = new System.Drawing.Size(41, 23);
			this.buttonFirstPage.TabIndex = 28;
			this.buttonFirstPage.Text = "<<";
			this.toolTip1.SetToolTip(this.buttonFirstPage, "Go First Page");
			this.buttonFirstPage.UseVisualStyleBackColor = true;
			this.buttonFirstPage.Click += new System.EventHandler(this.buttonFirstPage_Click);
			// 
			// buttonMovePreviousPage
			// 
			this.buttonMovePreviousPage.Location = new System.Drawing.Point(136, 140);
			this.buttonMovePreviousPage.Name = "buttonMovePreviousPage";
			this.buttonMovePreviousPage.Size = new System.Drawing.Size(41, 23);
			this.buttonMovePreviousPage.TabIndex = 29;
			this.buttonMovePreviousPage.Text = "<";
			this.toolTip1.SetToolTip(this.buttonMovePreviousPage, "Go Previous Page");
			this.buttonMovePreviousPage.UseVisualStyleBackColor = true;
			this.buttonMovePreviousPage.Click += new System.EventHandler(this.buttonMovePreviousPage_Click);
			// 
			// buttonMoveNextPage
			// 
			this.buttonMoveNextPage.Location = new System.Drawing.Point(183, 140);
			this.buttonMoveNextPage.Name = "buttonMoveNextPage";
			this.buttonMoveNextPage.Size = new System.Drawing.Size(41, 23);
			this.buttonMoveNextPage.TabIndex = 30;
			this.buttonMoveNextPage.Text = ">";
			this.toolTip1.SetToolTip(this.buttonMoveNextPage, "Go Next Page");
			this.buttonMoveNextPage.UseVisualStyleBackColor = true;
			this.buttonMoveNextPage.Click += new System.EventHandler(this.buttonMoveNextPage_Click);
			// 
			// buttonMoveLastPage
			// 
			this.buttonMoveLastPage.Location = new System.Drawing.Point(230, 140);
			this.buttonMoveLastPage.Name = "buttonMoveLastPage";
			this.buttonMoveLastPage.Size = new System.Drawing.Size(40, 23);
			this.buttonMoveLastPage.TabIndex = 31;
			this.buttonMoveLastPage.Text = ">>";
			this.toolTip1.SetToolTip(this.buttonMoveLastPage, "Go Last Page");
			this.buttonMoveLastPage.UseVisualStyleBackColor = true;
			this.buttonMoveLastPage.Click += new System.EventHandler(this.buttonMoveLastPage_Click);
			// 
			// textBoxCount
			// 
			this.textBoxCount.Location = new System.Drawing.Point(340, 143);
			this.textBoxCount.Name = "textBoxCount";
			this.textBoxCount.ReadOnly = true;
			this.textBoxCount.Size = new System.Drawing.Size(49, 20);
			this.textBoxCount.TabIndex = 33;
			// 
			// label10
			// 
			this.label10.AutoSize = true;
			this.label10.Location = new System.Drawing.Point(298, 146);
			this.label10.Name = "label10";
			this.label10.Size = new System.Drawing.Size(38, 13);
			this.label10.TabIndex = 32;
			this.label10.Text = "Count:";
			// 
			// textBoxZip
			// 
			this.textBoxZip.Location = new System.Drawing.Point(487, 87);
			this.textBoxZip.Name = "textBoxZip";
			this.textBoxZip.Size = new System.Drawing.Size(98, 20);
			this.textBoxZip.TabIndex = 35;
			// 
			// label11
			// 
			this.label11.AutoSize = true;
			this.label11.Location = new System.Drawing.Point(456, 89);
			this.label11.Name = "label11";
			this.label11.Size = new System.Drawing.Size(25, 13);
			this.label11.TabIndex = 34;
			this.label11.Text = "Zip:";
			// 
			// BayWindBrowser
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(609, 483);
			this.Controls.Add(this.textBoxZip);
			this.Controls.Add(this.label11);
			this.Controls.Add(this.textBoxCount);
			this.Controls.Add(this.label10);
			this.Controls.Add(this.buttonMoveLastPage);
			this.Controls.Add(this.buttonMoveNextPage);
			this.Controls.Add(this.buttonMovePreviousPage);
			this.Controls.Add(this.buttonFirstPage);
			this.Controls.Add(this.textBoxBayWindDBFilename);
			this.Controls.Add(this.label2);
			this.Controls.Add(this.buttonQuit);
			this.Controls.Add(this.buttonUpdate);
			this.Controls.Add(this.buttonSearch);
			this.Controls.Add(this.buttonDelete);
			this.Controls.Add(this.buttonCommit);
			this.Controls.Add(this.textBoxCountry);
			this.Controls.Add(this.label9);
			this.Controls.Add(this.textBoxState);
			this.Controls.Add(this.label8);
			this.Controls.Add(this.textBoxCity);
			this.Controls.Add(this.label7);
			this.Controls.Add(this.textBoxStreet);
			this.Controls.Add(this.label6);
			this.Controls.Add(this.textBoxPhone);
			this.Controls.Add(this.label5);
			this.Controls.Add(this.textBoxLastName);
			this.Controls.Add(this.label4);
			this.Controls.Add(this.textBoxFirstName);
			this.Controls.Add(this.label3);
			this.Controls.Add(this.buttonAdd);
			this.Controls.Add(this.label1);
			this.Controls.Add(this.dataGridViewPeople);
			this.Controls.Add(this.buttonInitDB);
			this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedToolWindow;
			this.Name = "BayWindBrowser";
			this.Text = "Bay Wind Browser";
			this.Load += new System.EventHandler(this.BayWindBrowser_Load);
			((System.ComponentModel.ISupportInitialize)(this.dataGridViewPeople)).EndInit();
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.Button buttonInitDB;
		private System.Windows.Forms.DataGridView dataGridViewPeople;
		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.Button buttonAdd;
		private System.Windows.Forms.Label label3;
		private System.Windows.Forms.TextBox textBoxFirstName;
		private System.Windows.Forms.TextBox textBoxLastName;
		private System.Windows.Forms.Label label4;
		private System.Windows.Forms.TextBox textBoxPhone;
		private System.Windows.Forms.Label label5;
		private System.Windows.Forms.TextBox textBoxStreet;
		private System.Windows.Forms.Label label6;
		private System.Windows.Forms.TextBox textBoxCity;
		private System.Windows.Forms.Label label7;
		private System.Windows.Forms.TextBox textBoxState;
		private System.Windows.Forms.Label label8;
		private System.Windows.Forms.TextBox textBoxCountry;
		private System.Windows.Forms.Label label9;
		private System.Windows.Forms.Button buttonCommit;
		private System.Windows.Forms.Button buttonDelete;
		private System.Windows.Forms.Button buttonSearch;
		private System.Windows.Forms.Button buttonUpdate;
		private System.Windows.Forms.Button buttonQuit;
		private System.Windows.Forms.Label label2;
		private System.Windows.Forms.TextBox textBoxBayWindDBFilename;
		private System.Windows.Forms.Button buttonFirstPage;
		private System.Windows.Forms.Button buttonMovePreviousPage;
		private System.Windows.Forms.Button buttonMoveNextPage;
		private System.Windows.Forms.Button buttonMoveLastPage;
		private System.Windows.Forms.TextBox textBoxCount;
		private System.Windows.Forms.Label label10;
		private System.Windows.Forms.TextBox textBoxZip;
		private System.Windows.Forms.Label label11;
		private System.Windows.Forms.ToolTip toolTip1;
	}
}